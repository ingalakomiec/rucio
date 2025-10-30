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

"""Consistency check for the auditor daemon"""

import logging

from datetime import datetime

from rucio.common.dumper import mkdir, smart_open, temp_file
from rucio.common.dumper.consistency import Consistency


#    ALGORITHM 1
#    an algorithm with lists and a dictionary:
#    fast (7 min for DESY dumps),
#    not suitable for big (>4GB) dumps

def consistency_check_fast(
    rucio_dump_before_path: str,
    rse_dump_path: str,
    rucio_dump_after_path: str
) -> ([],[]):

    logger = logging.getLogger('auditorqt.consistencycheck.consistency_check_fast')
    logger.debug("Consistency check - fast")

    rucio_dump_before = prepare_rucio_dump(rucio_dump_before_path)

    out = dict()

    i = 0

    for k in rucio_dump_before[0]:
        out[k]=16
        if rucio_dump_before[1][i]=='A':
            out[k]+=2
        i+=1

    del rucio_dump_before

    rse_dump = prepare_rse_dump(rse_dump_path)

    i = 0
    for k in rse_dump:
        if k in out:
            out[k]+=8
        else:
            out[k]=8

    del rse_dump

    rucio_dump_after = prepare_rucio_dump(rucio_dump_after_path)

    for k in rucio_dump_after[0]:
        if k in out:
            out[k]+=4
            if rucio_dump_after[1][i]=='A':
                out[k]+=1
        else:
            out[k]=4
        i+=1

    del rucio_dump_after

    missing_files = [k for k in out if out[k]==23]
    dark_files = [k for k in out if out[k]==8]

    results = (missing_files, dark_files)

    return results


#    ALGORITHM 2
#    an algorithm with open dump files and a dictionary:
#    fast, faster than ALGORITHM 1, 6.5 min for DESY dumps
#    not suitable for big (>4GB) dumps


def consistency_check_faster(
    rucio_dump_before_path: str,
    rse_dump_path: str,
    rucio_dump_after_path: str
) -> [[],[]]:

    logger = logging.getLogger('auditorqt.consistencycheck.consistency_check_faster')
    logger.debug("Consistency check - faster")

    out = dict()

    with smart_open(rucio_dump_before_path) as file_rucio_dump_before:

        for line in file_rucio_dump_before:
            parts = line.strip().split()
            key = parts[7]+'\n'
            out[key] = 16
            if parts[10]=='A':
                out[key]+=2

    with smart_open(rse_dump_path) as file_rse_dump:

        for line in file_rse_dump:
            if line in out:
                out[line]+=8
            else:
                out[line]=8

    with smart_open(rucio_dump_after_path) as file_rucio_dump_after:

        for line in file_rucio_dump_after:
            parts = line.strip().split()
            key = parts[7]+'\n'
            if key in out:
                out[key]+=4
                if parts[10]=='A':
                    out[key]+=1
            else:
                out[key]=4

    missing_files = [k for k in out if out[k]==23]
    dark_files = [k for k in out if out[k]==8]

    results = (missing_files, dark_files)

    return results

#    ALGORITHM 3
#    old algorithm
#    three dump files sorted opened
#    slow, 10.5 min for DESY dumps
#    suitable for big (>4GB) dumps


def consistency_check_slow_reliable(
    rucio_dump_before_path: str,
    rse_dump_path: str,
    rucio_dump_after_path: str,
    results_path: str,
    rse: str,
    date: datetime,
    cache_dir: str
) -> None:

    logger = logging.getLogger('auditorqt.consistencycheck.consistency_check_slow_reliable')
    logger.debug("Consistency check - slow, reliable")

    results = Consistency.dump(
        'consistency-manual',
        rse,
        rse_dump_path,
        rucio_dump_before_path,
        rucio_dump_after_path,
        date,
        cache_dir=cache_dir,
    )

    result_file_name = f"result.{rse}_{date:%Y%m%d}"

    with temp_file(results_path, final_name=result_file_name) as (output, _):
        for result in results:
            output.write('{0}\n'.format(result.csv()))

    return True

def prepare_rse_dump(
    dump_path: str
) -> []:

    logger = logging.getLogger('auditorqt.consistencycheck.prepare_rse_dump')
    logger.debug("Preparing RSE dump")

    file_rse_dump = smart_open(dump_path)
    rse_dump = file_rse_dump.readlines()
    file_rse_dump.close()

    return rse_dump

def prepare_rucio_dump(
    dump_path: str
) -> [[],[]]:

    logger = logging.getLogger('auditorqt.consistencycheck.prepare_rucio_dump')
    logger.debug("Preparing Rucio dump")

    rucio_dump = [[],[]]

    with smart_open(dump_path) as file_rucio_dump:

        for line in file_rucio_dump:
            rucio_dump[0].append(line.split()[7]+'\n')
            rucio_dump[1].append(line.split()[10])

        file_rucio_dump.close()


    return rucio_dump
