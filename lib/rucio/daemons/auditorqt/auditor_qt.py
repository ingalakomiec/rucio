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
The auditor daemon is the one responsible for the detection of inconsistencies on storage.
"""

#for benchmarking
import time

import functools
import logging
import os
import socket
import threading
from configparser import NoSectionError
from datetime import datetime
from typing import TYPE_CHECKING, Any, Optional

from rucio.common.logging import setup_logging
from rucio.common.config import config_get, config_has_section
from rucio.common.exception import RucioException
from rucio.core.heartbeat import sanity_check
from rucio.daemons.common import run_daemon
from rucio.client.rseclient import RSEClient
from rucio.common.exception import RSENotFound

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
    :param date:           The date of the RSE dump, for which the consistency check should be done.
    :param profile:        Which profile to use (default: atlas).
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
            no_declaration=no_declaration
        )
    )

def run_once(
    rses: str,
    keep_dumps: bool,
    delta: int,
    date: datetime,
    profile: str,
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
    :param date:              The date of the RSE dump, for which the consistency check should be done.
    :param profile:           Which profile to use (default: atlas).
    :param no_declaration:    No action on output (default: False).

    :param heartbeat_handler: A HeartbeatHandler instance.
    :param activity:          Activity to work on.
    :returns:                 A boolean flag indicating whether the daemon should go to sleep.
    """

    # for benchmarking
    start_time = time.perf_counter()

    worker_number, total_workers, logger = heartbeat_handler.live()

    rses_to_process = get_rses_to_process(rses)

    rses_names = [entry['rse'] for entry in rses_to_process]

    if len(rses_names) <= 0:
        raise RSENotFound("No RSE found to audit.")

    if not config_has_section('auditor'):
        raise NoSectionError("Auditor section required in config tu run te auditor daemon.")

    cache_dir = config_get('auditor', 'cache')
    results_dir = config_get('auditor', 'results')

    if not os.path.isdir(cache_dir):
        os.mkdir(cache_dir)

    if not os.path.isdir(results_dir):
        os.mkdir(results_dir)

    try:
        profile_maker = PROFILE_MAP[profile]
    except KeyError:
        logger(logging.ERROR, f"Invalid auditor profile name '{profile}'")

    # loop over all rses
    for rse in rses_names:
        try:
            profile = profile_maker(rse, keep_dumps, delta, date, cache_dir, results_dir, no_declaration)
        except RucioException:
            logger(logging.ERROR, f"Invalid configuration for profile '{profile}'")
            raise

    # for benchmarking
    end_time = time.perf_counter()
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time:.6f} seconds")


    return True


def run(
    rses: str,
    keep_dumps: bool = False,
    delta: int = 3,
    date: datetime = None,
    profile: str = "atlas",
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
    :param date:           The date of the RSE dump, for which the consistency check should be done.
    :param profile:        Which profile to use (default: atlas).
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
        logging.info('main: executing one iteration only')
        auditor_qt(rses, keep_dumps, delta, date, profile, sleep_time, once, no_declaration)
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
    rses: Optional["Iterable[str]"]
    ) -> Optional[list[dict[str, Any]]]:

    if rses:
        rses_to_process = RSEClient().list_rses(rses)
    else:
        rses_to_process = RSEClient().list_rses()

    return rses_to_process
