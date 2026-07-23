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

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from rucio.common.dumper import ddmendpoint_url, smart_open
from rucio.daemons.auditorqt.dumps import compare3, gnu_sort, parse_and_filter_file, parse_rse_dump, path_parsing_components, prepare_rse_dump

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

#    ALGORITHM 1
#    an algorithm with lists and a dictionary:
#    fast (7 min for DESY dumps),
#    not suitable for big (>4GB) dumps


def consistency_check_fast(
    rucio_dump_before_path: str,
    rse_dump_path: str,
    rucio_dump_after_path: str,
    parser: 'Callable' = lambda s: s
) -> tuple[list[str], list[str]]:

    logger = logging.getLogger('auditorqt.consistencycheck.consistency_check_fast')
    logger.debug("Consistency check - fast")

    rucio_dump_before = parser(rucio_dump_before_path)

    out = dict()

    i = 0

    for k in rucio_dump_before[0]:
        out[k] = 16

        if rucio_dump_before[1][i] == 'A':
            out[k] += 2
        i += 1

    del rucio_dump_before

    rse_dump = prepare_rse_dump(rse_dump_path)

    i = 0
    for k in rse_dump:
        if k in out:
            out[k] += 8
        else:
            out[k] = 8

    del rse_dump

    rucio_dump_after = parser(rucio_dump_after_path)

    for k in rucio_dump_after[0]:
        if k in out:
            out[k] += 4
            if rucio_dump_after[1][i] == 'A':
                out[k] += 1
        else:
            out[k] = 4
        i += 1

    del rucio_dump_after

    missing_files = [k for k in out if out[k] == 23]
    dark_files = [k for k in out if out[k] == 8]

    results = (missing_files, dark_files)

    return results


#    ALGORITHM 2
#    an algorithm with open dump files and a dictionary:
#    fast, faster than ALGORITHM 1, 6.5 min for DESY dumps
#    not suitable for big (>4GB) dumps


def consistency_check_faster(
    rucio_dump_before_path: str,
    rse_dump_path: str,
    rucio_dump_after_path: str,
    parser: 'Callable' = lambda s: s
) -> tuple[list[str], list[str]]:

    logger = logging.getLogger('auditorqt.consistencycheck.consistency_check_faster')
    logger.debug("Consistency check - faster")

    out = dict()

    file_rucio_dump_before = smart_open(rucio_dump_before_path)

    if file_rucio_dump_before is None:
        raise RuntimeError(f"Cannot open {rucio_dump_before_path}")

    with file_rucio_dump_before:
        for line in file_rucio_dump_before:
            key, status = parser(line)
            out[key] = 16
            if status == 'A':
                out[key] += 2

    file_rse_dump = smart_open(rse_dump_path)

    if file_rse_dump is None:
        raise RuntimeError(f"Cannot open {rse_dump_path}")

    with file_rse_dump:
        for line in file_rse_dump:
            line = line.strip()

            if line in out:
                out[line] += 8
            else:
                out[line] = 8

    file_rucio_dump_after = smart_open(rucio_dump_after_path)

    if file_rucio_dump_after is None:
        raise RuntimeError(f"Cannot open {rucio_dump_after_path}")

    with file_rucio_dump_after:
        for line in file_rucio_dump_after:
            key, status = parser(line)

            if key in out:
                out[key] += 4
                if status == 'A':
                    out[key] += 1
                else:
                    out[key] = 4

    missing_files = [k for k in out if out[k] == 23]
    dark_files = [k for k in out if out[k] == 8]

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
    rse: str,
    cache_dir: str,
    parser: 'Callable' = lambda s: s
) -> Iterator[tuple[str, str]]:

    logger = logging.getLogger('auditorqt.consistencycheck.consistency_check_slow_reliable')
    logger.debug("Consistency check - slow, reliable")

    rucio_dump_before_path_sorted = gnu_sort(
        parse_and_filter_file(rucio_dump_before_path, cache_dir=cache_dir, parser=parser),
        cache_dir=cache_dir,
        delimiter=',',
        fieldspec='1',
    )

    logger.debug("Rucio dump before sorted")

    rucio_dump_after_path_sorted = gnu_sort(
        parse_and_filter_file(rucio_dump_after_path, cache_dir=cache_dir, parser=parser),
        cache_dir,
        delimiter=',',
        fieldspec='1',
    )

    logger.debug("Rucio dump after sorted")

    prefix_components = path_parsing_components(ddmendpoint_url(rse))

    rse_dump_path_sorted = gnu_sort(
        parse_and_filter_file(
            rse_dump_path,
            cache_dir=cache_dir,
            parser=lambda line: parse_rse_dump(line, prefix_components),
        ),
        cache_dir=cache_dir,
    )

    logger.debug("RSE dump sorted")

    with open(rucio_dump_before_path_sorted) as prevf:
        with open(rucio_dump_after_path_sorted) as nextf:
            with open(rse_dump_path_sorted) as sdump:
                for path, where, status in compare3(prevf, sdump, nextf):
                    prevstatus, nextstatus = status
                    if where[0] and not where[1] and where[2]:
                        if prevstatus == 'A' and nextstatus == 'A':
                            yield ('MISSING', path)
                    if not where[0] and where[1] and not where[2]:
                        yield ('DARK', path)
