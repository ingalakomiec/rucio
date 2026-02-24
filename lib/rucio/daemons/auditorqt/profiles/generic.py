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

import glob
import logging
import hashlib
import os
import re
import shutil

from datetime import datetime, timedelta
from typing import Optional

from rucio.common.dumper import smart_open
from rucio.daemons.auditorqt.profiles.atlas_specific.dumps import remove_cached_dumps
from rucio.daemons.auditorqt.profiles.atlas_specific.output import process_output
from rucio.daemons.auditorqt.consistencycheck.consistency_check import consistency_check_fast, consistency_check_faster, consistency_check_slow_reliable

def generic_auditor(
        rse: str,
        keep_dumps: bool,
        delta: int,
        date: datetime,
        algorithm: str,
        cache_dir: str,
        results_dir: str,
        no_declaration: bool
) -> Optional[str]:

    """
    `rse` is the RSE name

    'keep_dumps' keep RSE and Rucio dumps on cache or not

    'delta' How many days older/newer than the RSE dump must the Rucio replica dumps be

    `date` is a datetime instance with the date of the desired dump or None
    to download the latest available dump

    'algorithm' - which algorithm to use to compare dumps;
    default: reliable

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

# big dumps
#    rse_dump_path = '/opt/rucio/lib/rucio/daemons/auditorqt/tmp/real_dumps/big_dumps/BNL-OSG2_DATADISK.dump_20250805'
#    rucio_dump_before_path = '/opt/rucio/lib/rucio/daemons/auditorqt/tmp/real_dumps/big_dumps/BNL-OSG2_DATADISK_2025-08-02.bz2'
#    rucio_dump_after_path = '/opt/rucio/lib/rucio/daemons/auditorqt/tmp/real_dumps/big_dumps/BNL-OSG2_DATADISK_2025-08-08.bz2'

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

    if algorithm == "fast":
        missing_files, dark_files = consistency_check_fast(rucio_dump_before_path_cache, rse_dump_path_cache, rucio_dump_after_path_cache)

    if algorithm == "faster":
        missing_files, dark_files = consistency_check_faster(rucio_dump_before_path_cache, rse_dump_path_cache, rucio_dump_after_path_cache)

    if algorithm in ("fast", "faster"):
        file_results = open(results_path, 'w')

        for k in range(len(dark_files)):
            file_results.write('DARK'+(dark_files[k]).replace("/",",",1))

        for k in range(len(missing_files)):
            file_results.write('MISSING'+(missing_files[k]).replace("/",",",1))

        file_results.close()


    if algorithm == "reliable":
        consistency_check_slow_reliable(rucio_dump_before_path_cache, rse_dump_path_cache, rucio_dump_after_path_cache, results_dir, rse, date, cache_dir)

    if no_declaration:
        logger.warning(f"No action on output performed")
    else:
        process_output(rse, results_path)

    if not keep_dumps:
        remove = glob.glob(f"{cache_dir}/*{rse}*")

        for fil in remove:
            os.remove(fil)

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

