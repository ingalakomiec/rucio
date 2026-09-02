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

"""ATLAS-specific auditor profile."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from rucio.common.dumper import temp_file
from rucio.daemons.auditorqt.consistencycheck.consistency_check import consistency_check_fast, consistency_check_faster, consistency_check_slow_reliable
from rucio.daemons.auditorqt.output import bz2_compress_file, remove_cached_dumps
from rucio.daemons.auditorqt.profiles.atlas.fetch_rse_dump import fetch_rse_dump
from rucio.daemons.auditorqt.profiles.atlas.fetch_rucio_dump import fetch_rucio_dump
from rucio.daemons.auditorqt.profiles.atlas.output import process_output
from rucio.daemons.auditorqt.profiles.atlas.prepare_dumps import parse_rucio_dump, prepare_path_and_status_to_sort, prepare_rucio_dump


def atlas_auditor(
        rse: str,
        keep_dumps: bool,
        delta: int,
        date: datetime | None,
        algorithm: str,
        cache_dir: str,
        results_dir: str,
        no_declaration: bool,
        compress_results: bool
) -> str | None:

    """
    'rse'- the RSE name

    'keep_dumps'-  keep RSE and Rucio dumps on cache or not

    'delta' - how many days older/newer than the RSE dump must the Rucio replica dumps be

    'date' - a datetime instance with the date of the desired dump;
    default: None; the latest RSE dump will be taken

    'algorithm' - which algorithm to use to compare dumps;
    default: reliable

    'cache_dir' -  dierectory where the dumps are cached

    'results_dir' - the directory where the results of the consistency check will be saved

    Return value: path to results
    """

    logger = logging.getLogger('atlas_auditor')

    if date is None:
        date = datetime.now()

    days = timedelta(delta)

    # fetching begin
    rse_dump_path_cache, date_rse = fetch_rse_dump(rse, cache_dir, date)
    rucio_dump_before_path_cache = fetch_rucio_dump(rse, date_rse - days, cache_dir)
    rucio_dump_after_path_cache = fetch_rucio_dump(rse, date_rse + days, cache_dir)
    # fetching end

    cached_dumps = [rucio_dump_before_path_cache, rse_dump_path_cache, rucio_dump_after_path_cache]

    result_file_name = f"result.{rse}_{date_rse:%Y%m%d}"
    results_path = f"{results_dir}/{result_file_name}"

    if os.path.exists(f"{results_path}") or os.path.exists(f"{results_path}.bz2"):
        logger.warning(f"Consistency check for {rse}, dump dated {date_rse:%d-%m-%Y}, already done. Skipping consistency check.")
        return results_path

    if algorithm == "fast":
        missing_files, dark_files = consistency_check_fast(rucio_dump_before_path_cache, rse_dump_path_cache, rucio_dump_after_path_cache, prepare_rucio_dump)

    if algorithm == "faster":
        missing_files, dark_files = consistency_check_faster(rucio_dump_before_path_cache, rse_dump_path_cache, rucio_dump_after_path_cache, parse_rucio_dump)

    if algorithm in ("fast", "faster"):
        file_results = open(results_path, 'w')

        for k in range(len(dark_files)):
            file_results.write('DARK' + (dark_files[k]).replace("/", ",", 1) + '\n')

        for k in range(len(missing_files)):
            file_results.write('MISSING' + (missing_files[k]).replace("/", ",", 1) + '\n')

        file_results.close()

    if algorithm == "reliable":
        results = consistency_check_slow_reliable(
            rucio_dump_before_path_cache,
            rse_dump_path_cache,
            rucio_dump_after_path_cache,
            rse,
            cache_dir=cache_dir,
            parser=prepare_path_and_status_to_sort
        )

        with temp_file(results_dir, final_name=result_file_name) as (output, _):

            for result in results:
                status, path = result
                output.write(status + (path).replace("/", ",", 1) + '\n')

    if not keep_dumps:
        remove_cached_dumps(cached_dumps)

    if no_declaration:
        logger.warning("No action on output performed")
    else:
        process_output(rse, results_path)

    if compress_results:
        results_path = bz2_compress_file(results_path)
        logger.debug(f"Compressed {results_path}")

    return results_path
