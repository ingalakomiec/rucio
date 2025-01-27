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
from typing import Optional

from rucio.common.logging import setup_logging
from rucio.core.heartbeat import sanity_check

GRACEFUL_STOP = threading.Event()
DAEMON_NAME = 'auditorqt'

def run(
    once: bool = False,
    sleep_time: int = 86400
) -> None:
    """
    Starts up the threads.

    :param once: Whether to execute once and exit.
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
            target=rse_decommissioner,
            kwargs={
                'sleep_time': sleep_time,
                'once': once
            },
        )
    ]
    [thread.start() for thread in threads]


def stop(
    signum: Optional[int] = None,
    frame: Optional["FrameType"] = None
) -> None:
    """
    Graceful exit.
    """
    GRACEFUL_STOP.set()
