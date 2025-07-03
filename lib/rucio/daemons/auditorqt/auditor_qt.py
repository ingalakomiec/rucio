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
The auditor daemon is the one responsible for the detection of inconsistencies
on storage, i.e.: dark data discovery.
"""

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
    nprocs: int,
    rses: str,
    keep_dumps: bool,
    delta: int,
    date: datetime,
    profile: str,
    once: bool,
    sleep_time: int
) -> None:
    """Daemon runner.

    :param nprocs:     Number of subprocesses, each subprocess checks a fraction of the DDM.
                       Endpoints in sequence (default: 1).
    :param rses:       RSEs to check specified as an RSE expression 
                       (default: check all RSEs).
    :param keep_dumps: Keep RSE and Rucio Replica Dumps on cache 
                       (default: False).
    :param delta:      How many days older/newer than the RSE dump
                       must the Rucio replica dumps be (default: 3).
    :param date:       The date of the RSE dump, for which the consistency check should be done.
    :param profile:    Which profile to use (default: atlas).
    :param once:       Whether to execute once and exit.
    :param sleep_time: Number of seconds to sleep before restarting.
    """
    run_daemon(
        once=once,
        graceful_stop=GRACEFUL_STOP,
        executable=DAEMON_NAME,
        partition_wait_time=1,
        sleep_time=sleep_time,
        run_once_fnc=functools.partial(
            run_once,
            nprocs=nprocs,
            rses=rses,
            keep_dumps=keep_dumps,
            delta=delta,
            date=date,
            profile=profile
        )
    )

def run_once(
    nprocs: int,
    rses: str,
    keep_dumps: bool,
    delta: int,
    date: datetime,
    profile: str,
    *,
    heartbeat_handler: 'HeartbeatHandler',
    activity: Optional[str]
) -> bool:
    """Add - what is auditor-QT doing?

    Auditor-QT - add a description

    worker number - number of worker threads; worker_number == 0 --> only one worker thread

    :param nprocs:            Number of subprocesses, each subprocess checks a fraction of the DDM.
                              Endpoints in sequence (default: 1).
    :param rses:              RSEs to check specified as an RSE expression
                              (default: check all RSEs).
    :param keep_dumps:        Keep RSE and Rucio Replica Dumps on cache
                              (default: False).
    :param delta:             How many days older/newer than the RSE dump
                              must the Rucio replica dumps be (default: 3).
    :param date:              The date of the RSE dump, for which the consistency check should be done.
    :param profile:           Which profile to use (default: atlas).
    :param heartbeat_handler: A HeartbeatHandler instance.
    :param activity:          Activity to work on.
    :returns:                 A boolean flag indicating whether the daemon should go to sleep.
    """
    worker_number, _, logger = heartbeat_handler.live()

    if nprocs < 1:
        raise RuntimeError("No Process to Run")

    rses_to_process = get_rses_to_process(rses)

    print("DATEEE:", date)

#    for rse in rses_to_process:
#        print(rse)

    rses_names = [entry['rse'] for entry in rses_to_process]
#    print(rses_names)

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
        logger(logging.ERROR, 'Invalid auditor profile name \''+profile+'\'')

# loop over all rses
    """
    for rse in rses_names:
        try:
            profile = profile_maker(nprocs, rse, keep_dumps, delta, cache_dir, results_dir)
        except RucioException:
            logger(logging.ERROR, 'Invalid configuration for profile \''+profile+'\'')
            raise
    """
#without an rse loop - just for tests

#    rse='AMAZON-BOTO'
#s3://s3.amazonaws.com:80rucio_bucketdumps/dump_20250515


    rse='XRD1'
#root://xrd1:1094//ruciodumps/dump_20250515


    try:
       profile = profile_maker(nprocs, rse, keep_dumps, delta, date, cache_dir, results_dir)
    except RucioException:
        logger(logging.ERROR, 'Invalid configuration for profile \''+profile+'\'')
        raise

    return True


def run(
    nprocs: int,
    rses: str,
    keep_dumps: bool = False,
    delta: int = 3,
    date: datetime = None,
    profile: str = "atlas",
    once: bool = False,
    sleep_time: int = 86400
) -> None:
    """
    Starts up the auditor-qt threads.

    :param nprocs:     Number of subprocesses, each subprocess checks a fraction of the DDM.
                       Endpoints in sequence (default: 1).
    :param rses:       RSEs to check specified as an RSE expression
                       (default: check all RSEs).
    :param keep_dumps: Keep RSE and Rucio Replica Dumps on cache
                       (default: False).
    :param delta:      How many days older/newer than the RSE dump
                       must the Rucio replica dumps be (default: 3).
    :param date:       The date of the RSE dump, for which the consistency check should be done.
    :param profile:    Which profile to use (default: atlas).
    :param once:       Whether to execute once and exit.
    :param sleep_time: Number of seconds to sleep before restarting.
    """

    setup_logging(process_name=DAEMON_NAME)
    hostname = socket.gethostname()
    sanity_check(executable='rucio-auditorqt', hostname=hostname)

    logging.info('Auditor-QT starting 1 thread')

    # Creating only one thread but putting it in a list to conform to how
    # other daemons are run.
    threads = [
        threading.Thread(
            target=auditor_qt,
            kwargs={
                'nprocs': nprocs,
                'rses': rses,
                'keep_dumps': keep_dumps,
                'delta': delta,
                'date': date,
                'profile': profile,
                'sleep_time': sleep_time,
                'once': once
            },
        )
    ]
    [thread.start() for thread in threads]
    # Interruptible joins require a timeout.
    while any(thread.is_alive() for thread in threads):
        [thread.join(timeout=3.14) for thread in threads]


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

# list_rses as in reaper
#    rses_to_process_reaper = list_rses()
#    print("RSEs to process REAPER VERSION")
#    print(rses_to_process_reaper)

    return rses_to_process

