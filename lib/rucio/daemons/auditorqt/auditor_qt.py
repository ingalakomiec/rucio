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

import logging
import socket
import threading
from typing import TYPE_CHECKING, Optional

from rucio.common.logging import setup_logging
from rucio.core.heartbeat import sanity_check
from rucio.daemons.common import run_daemon

GRACEFUL_STOP = threading.Event()
DAEMON_NAME = 'auditorqt'

if TYPE_CHECKING:
    from types import FrameType

    from rucio.common.types import LoggerFunction
    from rucio.daemon.common import HeartbeatHandler

def auditor_qt(
    once: bool,
    sleep_time: int,
    nprocs: int,
    rses: str,
    keep_dumps: bool,
    delta: int
) -> None:
    """Daemon runner.

    :param once: Whether to execute once and exit.
    :param sleep_time: Number of seconds to sleep before restarting.
    """
#    run_daemon(
#        once=once,
#        graceful_stop=GRACEFUL_STOP,
#        executable=DAEMON_NAME,
#        partition_wait_time=1,
#        sleep_time=sleep_time,
#        run_once_fnc=run_once
#    )
    run_once_tmp()

def run_once(
    *,
    heartbeat_handler: 'HeartbeatHandler',
    activity: Optional[str]
) -> bool:
    """Add - what is auditor-QT doing?
    
    Auditor-QT - add a description

    worker number - number of worker threads; worker_number == 0 --> only one worker thread 
    
    :param heartbeat_handler: A HeartbeatHandler instance.
    :param activity: Activity to work on.
    :returns: A boolean flag indicating whether the daemon should go to sleep.
    """
#    worker_number, _, logger = heartbeat_handler.live()

#    if worker_number != 0:
#        logger(logging.INFO, 'RSE decommissioner thread id is not 0, will sleep.'
#               ' Only thread 0 will work')
#        return True

    # Collect all RSEs with the 'decommission' attribute
#    rses = get_rses_with_attribute(RseAttr.DECOMMISSION)
#    random.shuffle(rses)

#    for rse in rses:
        # Get the decommission attribute (encodes the decommissioning config)
#        attr = get_rse_attribute(rse['id'], RseAttr.DECOMMISSION)
#        try:
#            config = attr_to_config(attr)  # type: ignore (attr could be None)
#        except InvalidStatusName:
#            logger(logging.ERROR, 'RSE %s has an invalid decommissioning status',
#                   rse['rse'])
#            continue

#        if config['status'] != DecommissioningStatus.PROCESSING:
#            logger(logging.INFO, 'Skipping RSE %s which has decommissioning status "%s"',
#                   config['status'])
#            continue

#        try:
#            profile_maker = PROFILE_MAP[config['profile']]
#        except KeyError:
#            logger(logging.ERROR, 'Invalid decommissioning profile name %s used for %s',
#                   config['profile'], rse['rse'])
#            continue

#        try:
#            profile = profile_maker(rse, config)
#        except RucioException:
#            logger(logging.ERROR, 'Invalid configuration for profile %s', config['profile'])
#            raise

#        logger(logging.INFO, 'Decommissioning %s: %s', rse['rse'], attr)
#        try:
#            decommission_rse(rse, profile, logger=logger)
#        except Exception as error:  # pylint: disable=broad-exception-caught
#            logger(logging.ERROR, 'Unexpected error while decommissioning %s: %s',
#                   rse['rse'], str(error), exc_info=True)

    return True

def run_once_tmp() -> bool:

    #print Hello world
    print("Hello world")

    #fetch input
    fetch_input()

    return True

def run(
    once: bool = False,
    sleep_time: int = 86400
   # nprocs: int = nprocs
) -> None:
    """
    Starts up the threads.

    :param once: Whether to execute once and exit.
    :param sleep_time: Number of seconds to sleep before restarting.
    """

    setup_logging(process_name=DAEMON_NAME)
    hostname = socket.gethostname()
#    sanity_check(executable='rucio-auditorqt', hostname=hostname)

    logging.info('Auditor-QT starting 1 thread')

    # Creating only one thread but putting it in a list to conform to how
    # other daemons are run.
    threads = [
        threading.Thread(
            target=auditor_qt,
            kwargs={
                'sleep_time': sleep_time,
                'once': once
#                'nprocs': nprocs,
#                'rses': rses,
#                'keep_dumps': keep_dumps,
#                'delta': delta
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

def fetch_input():
    print("fetching input")
# Collect all RSEs with the 'decommission' attribute
#    rses = get_rses_with_attribute(RseAttr.DECOMMISSION)
#    random.shuffle(rses)

