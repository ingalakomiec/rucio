# Copyright European Organization for Nuclear Research (CERN) since 2012
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
The auditor daemon is responsible for the detection of inconsistencies on storage:
- fetching Rucio and RSE dumps for the 'atlas' profile; for the 'generic' profile
  paths to dumps are given as strings,
- making the consistency check (three algorithms are available),
- reporting dark and missing replicas.
"""

import argparse
import functools
import logging
import os
import socket
import time
import threading
from configparser import NoSectionError
from datetime import datetime
from typing import TYPE_CHECKING, Any, Iterable, Optional

from rucio.common.logging import setup_logging
from rucio.common.config import config_get, config_has_section
from rucio.common.exception import RucioException
from rucio.core.heartbeat import sanity_check
from rucio.daemons.common import run_daemon
from rucio.client.rseclient import RSEClient
from rucio.common.exception import RSENotFound

from .consistencycheck import ALGORITHM_MAP
from .profiles import PROFILE_MAP

GRACEFUL_STOP = threading.Event()
DAEMON_NAME = 'auditorqt'

if TYPE_CHECKING:
    from types import FrameType

    from rucio.common.types import LoggerFunction
    from rucio.daemon.common import HeartbeatHandler

def auditor_qt(
    rses: str,
    keep_dumps: bool,
    delta: int,
    date: datetime,
    profile: str,
    algorithm: str,
    no_declaration: bool,
    once: bool,
    sleep_time: int
) -> None:
    """Daemon runner.
    :param rses:           RSEs to check specified as an RSE expression
                           (default: check all RSEs).
    :param keep_dumps:     Keep RSE and Rucio Replica Dumps on cache
                           (default: False).
    :param delta:          How many days older/newer than the RSE dump
                           must the Rucio replica dumps be (default: 3).
    :param date:           The date of the RSE dump, for which the consistency check should be done
                           (default: None; the newest RSE dump will be taken).
    :param profile:        Which profile to use (default: atlas).
    :param algorithm:      Which algorithm to use to compare dumps (default: reliable).
    :param no_declaration: No action on output (default: False).
    :param once:           Whether to execute once and exit.
    :param sleep_time:     Thread sleep time after each chunk of work.
    """
    run_daemon(
        once=once,
        graceful_stop=GRACEFUL_STOP,
        executable=DAEMON_NAME,
        partition_wait_time=1,
        sleep_time=sleep_time,
        run_once_fnc=functools.partial(
            run_once,
            rses=rses,
            keep_dumps=keep_dumps,
            delta=delta,
            date=date,
            profile=profile,
            algorithm=algorithm,
            no_declaration=no_declaration
        )
    )

def run_once(
    rses: str,
    keep_dumps: bool,
    delta: int,
    date: datetime,
    profile: str,
    algorithm: str,
    no_declaration: bool,
    *,
    heartbeat_handler: 'HeartbeatHandler',
    activity: Optional[str]
) -> bool:
    """
    :param rses:              RSEs to check specified as an RSE expression
                              (default: check all RSEs).
    :param keep_dumps:        Keep RSE and Rucio Replica Dumps on cache
                              (default: False).
    :param delta:             How many days older/newer than the RSE dump
                              must the Rucio replica dumps be (default: 3).
    :param date:              The date of the RSE dump, for which the consistency check should be done
                              (default: None; the newest RSE dump will be taken).
    :param profile:           Which profile to use (default: atlas).
    :param algorithm:         Which algorithm to use to compare dumps (default: reliable).
    :param no_declaration:    No action on output (default: False).

    :param heartbeat_handler: A HeartbeatHandler instance.
    :param activity:          Activity to work on.
    :returns:                 A boolean flag indicating whether the daemon should go to sleep.
    """

    start_time = time.perf_counter()

    worker_number, total_workers, logger = heartbeat_handler.live()

    rses_to_process = get_rses_to_process(rses)

    rses_names = [entry.get('rse') for entry in rses_to_process if 'rse' in entry]

    if not rses_names:
        raise RSENotFound("No RSE found to audit.")

    if not config_has_section('auditor'):
        raise NoSectionError("Auditor section required in config tu run te auditor daemon.")

    cache_dir = config_get('auditor', 'cache')
    results_dir = config_get('auditor', 'results')

    os.makedirs(cache_dir, exist_ok=True)
    os.makedirs(results_dir, exist_ok=True)

    try:
        profile_maker = PROFILE_MAP[profile]
    except KeyError as exc:
        raise ValueError(f"Invalid auditor profile name '{profile}'") from exc

    try:
        algorithm_maker = ALGORITHM_MAP[algorithm]
    except KeyError as exc:
        raise ValueError(f"Invalid auditor algorithm name '{algorithm}'") from exc


    # loop over all rses
    for rse in rses_names:
        try:
            profile_instance = profile_maker(rse, keep_dumps, delta, date, algorithm, cache_dir, results_dir, no_declaration)
        except RucioException:
            logger(logging.ERROR, f"Invalid configuration for profile '{profile}'")

    end_time = time.perf_counter()
    execution_time = end_time - start_time
    logger(logging.INFO, f"Execution time: {execution_time:.6f} seconds")

    return True


def run(
    rses: str,
    keep_dumps: bool = False,
    delta: int = 3,
    date: datetime = None,
    profile: str = "atlas",
    algorithm: str = "reliable",
    no_declaration: bool = False,
    once: bool = False,
    threads: int = 1,
#    sleep_time: int = 86400
    sleep_time: int = 60
) -> None:
    """
    Starts up the auditor-qt threads.

    :param rses:           RSEs to check specified as an RSE expression
                           (default: check all RSEs).
    :param keep_dumps:     Keep RSE and Rucio Replica Dumps on cache
                           (default: False).
    :param delta:          How many days older/newer than the RSE dump
                           must the Rucio replica dumps be (default: 3).
    :param date:           The date of the RSE dump, for which the consistency check should be done
                           (default: None; the newest RSE dump will be taken).
    :param profile:        Which profile to use (default: atlas).
    :param algorithm:      Which algorithm to use to compare dumps (default: reliable).
    :param no_declaration: No action on output (default: False).
    :param once:           Whether to execute once and exit.
    :param threads:        Number of threads for this process
                           (default: 1).
    :param sleep_time:     Number of seconds to sleep before restarting.
    """

    setup_logging(process_name=DAEMON_NAME)
    hostname = socket.gethostname()
    sanity_check(executable='rucio-auditorqt', hostname=hostname)

    if threads < 1:
        raise RuntimeError("Number of threads < 1")

    if once:
        logging.info('Auditor-QT: executing one iteration only')
        auditor_qt(
            rses=rses,
            keep_dumps=keep_dumps,
            delta=delta,
            date=date,
            profile=profile,
            algorithm=algorithm,
            no_declaration=no_declaration,
            once=once,
            sleep_time=sleep_time,
        )
    else:
        logging.info("Auditor-QT starting threads")
        thread_list = [
            threading.Thread(
                target=auditor_qt,
                kwargs={
                    'rses': rses,
                    'keep_dumps': keep_dumps,
                    'delta': delta,
                    'date': date,
                    'profile': profile,
                    'algorithm': algorithm,
                    'no_declaration': no_declaration,
                    'once': once,
                    'sleep_time': sleep_time
                },
            )
            for i in range(0, threads)
        ]
        [thread.start() for thread in thread_list]

        # Interruptible joins require a timeout.
        while thread_list[0].is_alive():
            [thread.join(timeout=3.14) for thread in thread_list]

def stop(
    signum: Optional[int] = None,
    frame: Optional["FrameType"] = None
) -> None:
    """
    Graceful exit.
    """
    GRACEFUL_STOP.set()

def get_rses_to_process(
    rses: Optional[str]
    ) -> list[dict[str, Any]]:

    if rses:
        return  RSEClient().list_rses(rses)
    else:
        return RSEClient().list_rses()

def parse_date(date: str) -> datetime:
    try:
        return datetime.strptime(date, '%Y-%m-%d')
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "Date must be in YYYY-MM-DD format"
        ) from exc
