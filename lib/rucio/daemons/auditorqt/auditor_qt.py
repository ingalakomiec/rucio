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
import socket
import threading
from typing import TYPE_CHECKING, Any, Optional

from rucio.common.logging import setup_logging
from rucio.core.heartbeat import sanity_check
from rucio.daemons.common import run_daemon
from rucio.client.rseclient import RSEClient
from rucio.core.rse import list_rses

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
        )
    )

def run_once(
    nprocs: int,
    rses: str,
    keep_dumps: bool,
    delta: int,
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
    :param heartbeat_handler: A HeartbeatHandler instance.
    :param activity:          Activity to work on.
    :returns:                 A boolean flag indicating whether the daemon should go to sleep.
    """
    worker_number, _, logger = heartbeat_handler.live()

    #print Hello world
    #print("Hello world")
    # print parameters' values
    #print(nprocs)
    #print(rses)
    #print(keep_dumps)
    #print(delta)

    if nprocs < 1:
        raise RuntimeError("No Process to Run")

    rses_to_process = get_rses_to_process(rses)

    print("RSEs to process")
    print(rses_to_process)

    print("in run_once")

    for rse in rses_to_process:
        print(rse)


    #fetch input
    rse_dump = fetch_rse_dumps()
    print('RSE dump:')
    print(rse_dump)

    print('Rucio dumps:')
    rucio_dump_before, rucio_dump_after = fetch_rucio_dumps()

    print('before')
    print(rucio_dump_before)

    print('after')
    print(rucio_dump_after)
    consistency_check('rucio_dump_before', 'rse_dump',
    'rucio_dump_after', 'results')


    return True


def run(
    nprocs: int,
    rses: str,
    keep_dumps: bool = False,
    delta: int = 3,
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
    print("getting rses to process")

    if rses is None:
        rses_to_process = RSEClient().list_rses()
    else:
        rses_to_process = RSEClient().list_rses(rses)

#    rses_to_process = list_rses()

    for rse in rses_to_process:
        print(rse)

    print("in get_rses ...")
    print(rses_to_process)

    return rses_to_process

def fetch_rse_dumps():

    print("fetching RSE dumps")
# Collect all RSEs with the 'decommission' attribute
#    rses = get_rses_with_attribute(RseAttr.DECOMMISSION)
#    random.shuffle(rses)

    file_rse_dump = open('/opt/rucio/lib/rucio/daemons/auditorqt/tmp/rse_dump', 'rt')
    rse_dump = file_rse_dump.readlines()
    file_rse_dump.close()

    return rse_dump

def fetch_rucio_dumps():

    print("fetching Rucio dumps")

    file_rucio_dump_before = open('/opt/rucio/lib/rucio/daemons/auditorqt/tmp/rucio_dump_before', 'rt')
    file_rucio_dump_after = open('/opt/rucio/lib/rucio/daemons/auditorqt/tmp/rucio_dump_after', 'rt')

    rucio_dump_before = file_rucio_dump_before.readlines()
    rucio_dump_after = file_rucio_dump_after.readlines()

    file_rucio_dump_before.close()
    file_rucio_dump_after.close()
    return (rucio_dump_before, rucio_dump_after)

def consistency_check(
    rucio_dump_before,
    rse_dump,
    rucio_dump_after,
    results):

    print("consistency check")

    return results
