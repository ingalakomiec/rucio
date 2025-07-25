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

"""Generic auditor profiles."""

import logging
import hashlib
import os
import re
import shutil

from datetime import datetime, timedelta
from typing import Optional

from rucio.common.dumper import smart_open
from rucio.daemons.auditorqt.profiles.atlas_specific.dumps import remove_cached_dumps
#from rucio.daemons.auditorqt.profiles.atlas_specific.output import process_output

def generic_auditor(
        rse: str,
        keep_dumps: bool,
        delta: int,
        date: datetime,
        cache_dir: str,
        results_dir: str
) -> Optional[str]:

    """
    `rse` is the RSE name

    'keep_dumps' keep RSE and Rucio dumps on cache or not

    'delta' How many days older/newer than the RSE dump must the Rucio replica dumps be

    `date` is a datetime instance with the date of the desired dump or None
    to download the latest available dump

    'cache_dir' dierectory where the dumps are cached

    `results_dir` is the directory where the results of the consistency check will be saved

    Return value: path to results
    """

    logger = logging.getLogger('generic_auditor')

    if date is None:
        date = datetime.now()

    delta = timedelta(delta)

#   paths to rse and rucio dumps
    rse_dump_path = '/opt/rucio/lib/rucio/daemons/auditorqt/tmp/real_dumps/dump_20250127.bz2'
    rucio_dump_before_path = '/opt/rucio/lib/rucio/daemons/auditorqt/tmp/real_dumps/rucio_dump_before/rucio_before.DESY-ZN_DATADISK_2025-01-24.bz2'
    rucio_dump_after_path = '/opt/rucio/lib/rucio/daemons/auditorqt/tmp/real_dumps/rucio_dump_after/rucio_after.DESY-ZN_DATADISK_2025-01-30.bz2'

    rse_dump_path_cache, date_rse = fetch_rse_dump(rse_dump_path, rse, cache_dir, date)
    rucio_dump_before_path_cache = fetch_rucio_dump(rucio_dump_before_path, rse, date_rse - delta, cache_dir)
    rucio_dump_after_path_cache = fetch_rucio_dump(rucio_dump_after_path, rse, date_rse + delta, cache_dir)

    cached_dumps = [rucio_dump_before_path_cache, rse_dump_path_cache, rucio_dump_after_path_cache]

    result_file_name = f"result.{rse}_{date:%Y%m%d}"
    results_path = f"{results_dir}/{result_file_name}"

    if os.path.exists(f"{results_path}") or os.path.exists(f"{results_path}.bz2"):
        logger.warning(f"Consistency check for {rse}, dump dated {date_rse:%d-%m-%Y}, already done. Skipping consistency check.")
        if not keep_dumps:
            remove_cached_dumps(cached_dumps)
        return results_path

    missing_files, dark_files = consistency_check(rucio_dump_before_path_cache, rse_dump_path_cache, rucio_dump_after_path_cache)

    file_results = open(results_path, 'w')

    for k in range(len(dark_files)):
        file_results.write('DARK'+(dark_files[k]).replace("/",",",1))

#missing
    for k in range(len(missing_files)):
        file_results.write('MISSING'+(missing_files[k]).replace("/",",",1))

    file_results.close()

    # taken from the atlas profile
#    process_output(rse, results_path)

    if not keep_dumps:
        # taken from the atlas profile
        remove_cached_dumps(cached_dumps)

    return results_path

def fetch_rse_dump(
    source_path: str,
    rse: str,
    cache_dir: str,
    date: Optional[datetime] = None,
    ) -> tuple[str, datetime]:

    logger = logging.getLogger('auditor.fetch_rse_dump')

    if date is None:
        date = datetime.now()

    # hash added to get a distinct file name
    hash = hashlib.sha1(source_path.encode()).hexdigest()
    filename = f"ddmendpoint_{rse}_{date:%d-%m-%Y}_{hash}"
    filename = re.sub(r'\W', '-', filename)
    final_path = f"{cache_dir}/{filename}"

    shutil.copyfile(source_path, final_path)

    logger.debug(f"RSE dump taken from: {source_path} and cached in: {final_path}")

    return (final_path, date)

def fetch_rucio_dump(
    source_path: str,
    rse: str,
    date: "datetime",
    cache_dir: str
) -> str:

    logger = logging.getLogger('auditor.fetch_rucio_dump')

    # hash added to get a distinct file name
    hash = hashlib.sha1(source_path.encode()).hexdigest()
    filename = f"{rse}_{date:%d-%m-%Y}_{hash}"
    filename = re.sub(r'\W', '-', filename)
    final_path = f"{cache_dir}/{filename}"

    shutil.copyfile(source_path, final_path)

    logger.debug(f"Rucio dump before taken from: {source_path} and cached in: {final_path}")

    return final_path


def prepare_rse_dump(
    dump_path: str
) -> []:

    logger = logging.getLogger('auditor.prepare_rse_dump')
    logger.debug("Preparing RSE dump")

    file_rse_dump = smart_open(dump_path)
    rse_dump = file_rse_dump.readlines()
    file_rse_dump.close()

    return rse_dump


def prepare_rucio_dump(
    dump_path: str
) -> [[],[]]:

    logger = logging.getLogger('auditor.prepare_rucio_dump')
    logger.debug("Preparing Rucio dump")

    rucio_dump = [[],[]]

    with smart_open(dump_path) as file_rucio_dump:

        for line in file_rucio_dump:
            rucio_dump[0].append(line.split()[7]+'\n')
            rucio_dump[1].append(line.split()[10])

        file_rucio_dump.close()


    return rucio_dump

def consistency_check(
    rucio_dump_before_path: str,
    rse_dump_path: str,
    rucio_dump_after_path: str
) -> ([],[]):

    logger = logging.getLogger('auditor.consistency_check')
    logger.debug("Consistncy check")

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
