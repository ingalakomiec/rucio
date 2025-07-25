#!/usr/bin/env python3
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

"""ATLAS-specific OLD auditor profile."""

import argparse
import logging
import logging.handlers
import os
import signal
import sys
import textwrap
import time
from configparser import NoSectionError
from datetime import datetime
from functools import partial
from multiprocessing import Event, Pipe, Process, Queue

import rucio.common.config as config
import rucio.common.dumper as dumper
#import rucio.daemons.auditor
from rucio.client.rseclient import RSEClient
from rucio.common.exception import RSENotFound

RETRY_AFTER = 60 * 60 * 24 * 14  # Two weeks

from rucio.common.dumper import DUMPS_CACHE_DIR

def setup_pipe_logger(pipe, loglevel):
    logger = logging.getLogger('auditor')
    logger.setLevel(loglevel)
    handler = dumper.LogPipeHandler(pipe)
    logger.addHandler(handler)

    formatter = logging.Formatter(
        "%(asctime)s  %(name)-22s  %(levelname)-8s %(message)s"
    )
    handler.setFormatter(formatter)
    return logger


def function(
    nprocs: int,
    rses: str,
    keep_dumps: bool,
    delta: int,
) -> None:

    import rucio.daemons.auditor

    if nprocs < 1:
        raise RuntimeError("No Processes to Run")

    if rses is None:
        rses_gen = RSEClient().list_rses()
    else:
        rses_gen = RSEClient().list_rses(rses)

    rses = [entry['rse'] for entry in rses_gen]
    if len(rses) <= 0:
        raise RSENotFound("No RSEs found to audit.")

    procs = []
    queue = Queue()
    retry = Queue()
    terminate = Event()
    logpipes = []

    loglevel = logging.getLevelName(config.config_get('common', 'loglevel'))

    mainlogr, mainlogw = Pipe(duplex=False)
    logpipes.append(mainlogr)
    logger = setup_pipe_logger(mainlogw, loglevel)

    if not config.config_has_section('auditor'):
        raise NoSectionError("Auditor section required in config to run the auditor daemon.")

    cache_dir = config.config_get('auditor', 'cache')
    results_dir = config.config_get('auditor', 'results')

    logfilename = os.path.join(config.config_get('common', 'logdir'), 'auditor.log')
    logger.info('Starting auditor')

    def termhandler(sign, trace):
        logger.error('Main process received signal %d, terminating child processes', sign)
        terminate.set()
        for proc in procs:
            proc.join()

#    signal.signal(signal.SIGTERM, termhandler)

    for n in range(nprocs):
        logpiper, logpipew = Pipe(duplex=False)
        p = Process(
            target=partial(
                rucio.daemons.auditor.check,
                queue,
                retry,
                terminate,
                logpipew,
                cache_dir,
                results_dir,
                keep_dumps,
                delta,
            ),
            name='auditor-worker'
        )
        p.start()
        procs.append(p)
        logpipes.append(logpiper)

    p = Process(
        target=partial(
            rucio.daemons.auditor.activity_logger,
            logpipes,
            logfilename,
            terminate
        ),
        name='auditor-logger'
    )
    p.start()
    procs.append(p)

    last_run_month = None  # Don't check more than once per month. FIXME: Save on DB or file...

    try:
        while all(p.is_alive() for p in procs):
            while last_run_month == datetime.utcnow().month:
                time.sleep(60 * 60 * 24)

            for rse in rses:
                queue.put((rse, 1))

            time.sleep(RETRY_AFTER)

            # Avoid infinite loop if an alternative check() implementation doesn't
            # decrement the number of attempts and keeps pushing failed checks.
            tmp_list = []
            while not retry.empty():
                tmp_list.append(retry.get())

            for each in tmp_list:
                queue.put(each)

    except:
        logging.error('Main process failed: %s', sys.exc_info()[0])

    terminate.set()
    for proc in procs:
        proc.join()


def get_parser():
    """
    Returns the argparse parser.
    """
    parser = argparse.ArgumentParser(description="The auditor daemon is the one responsible for the detection of inconsistencies on storage, i.e.: dark data discovery.",
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        '--nprocs',
        help='Number subprocess, each subprocess check a fraction of the DDM '
             'Endpoints in sequence (default: 1).',
        default=1,
        type=int,
    )
    parser.add_argument(
        '--rses',
        help='RSEs to check specified as a RSE expression, defaults to check '
             'all the RSEs known to Rucio (default: check all RSEs).',
        default=None,
        type=str,
    )
    parser.add_argument(
        '--keep-dumps',
        help='Keep RSE and Rucio Replica Dumps on cache '
             '(default: False).',
        action='store_true',
    )
    parser.add_argument(
        '--delta',
        help='How many days older/newer than the RSE dump must the Rucio replica dumps be '
             '(default: 3).',
        default=3,
        type=int,
    )
    parser.epilog = textwrap.dedent(r"""
        examples:
            # Check all RSEs using only 1 subprocess
            %(prog)s

            # Check all SCRATCHDISKs with 4 subprocesses
            %(prog)s --nprocs 4 --rses "type=SCRATCHDISK"

            # Check all Tier 2 DATADISKs, except "BLUE_DATADISK" and "RED_DATADISK"
            %(prog)s --rses "tier=1&type=DATADISK\(BLUE_DATADISK|RED_DATADISK)"
    """)
    return parser


#if __name__ == '__main__':

def atlas_auditor_old(
        rse: str,
        keep_dumps: bool,
        delta: int,
        date: datetime,
        cache_dir: str,
        results_dir: str
) -> None:

    nprocs = 1
    function(nprocs,
        rse,
        keep_dumps,
        delta
    )

    return True
