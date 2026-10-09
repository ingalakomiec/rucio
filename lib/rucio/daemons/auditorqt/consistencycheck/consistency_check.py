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
from rucio.daemons.auditorqt.consistencycheck.algorithm3.compare import compare3
from rucio.daemons.auditorqt.consistencycheck.algorithm3.parse_dumps import parse_and_filter_file, path_parsing_components
from rucio.daemons.auditorqt.consistencycheck.algorithm3.sort_dumps import gnu_sort

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

#    ALGORITHM 1
#    an algorithm with lists and a dictionary:
#    fast (7 min for DESY dumps),
#    not suitable for big (>4GB) dumps
#    paths and statuses are keps in memory


def consistency_check_fast(
    rucio_dump_before_path: str,
    rse_dump_path: str,
    rucio_dump_after_path: str,
    rucio_dump_parser: 'Callable' = lambda s: s,
    rse_dump_parser: 'Callable' = lambda s: s
) -> tuple[list[str], list[str]]:

    logger = logging.getLogger('auditorqt.consistencycheck.consistency_check_fast')
    logger.debug("Consistency check - fast")

    paths = []
    statuses = []

    file_rucio_dump_before = smart_open(rucio_dump_before_path)

    if file_rucio_dump_before is None:
        raise RuntimeError(f"Cannot open {rucio_dump_before_path}")

    # read and parse the dump line by line, keeping paths and statuses in memory
    with file_rucio_dump_before:
        for line in file_rucio_dump_before:
            path, status = rucio_dump_parser(line)
            paths.append(path)
            statuses.append(status)

    out = dict()

    i = 0

    for k in paths:
        out[k] = 16

        if statuses[i] == 'A':
            out[k] += 2
        i += 1

    del paths
    del statuses

    file_rse_dump = smart_open(rse_dump_path)

    if file_rse_dump is None:
        raise RuntimeError(f"Cannot open {rse_dump_path}")

    rse_dump = []

    with file_rse_dump:
        for line in file_rse_dump:
            rse_dump.append(rse_dump_parser(line))

    i = 0
    for k in rse_dump:
        if k in out:
            out[k] += 8
        else:
            out[k] = 8

    del rse_dump

    paths = []
    statuses = []

    file_rucio_dump_after = smart_open(rucio_dump_after_path)

    if file_rucio_dump_after is None:
        raise RuntimeError(f"Cannot open {rucio_dump_after_path}")

    # read and parse the dump line by line, keeping paths and statuses in memory
    with file_rucio_dump_after:
        for line in file_rucio_dump_after:
            path, status = rucio_dump_parser(line)
            paths.append(path)
            statuses.append(status)

    i = 0

    for k in paths:
        if k in out:
            out[k] += 4
            if statuses[i] == 'A':
                out[k] += 1
        else:
            out[k] = 4
        i += 1

    del paths
    del statuses

    missing_files = [k for k in out if out[k] == 23]
    dark_files = [k for k in out if out[k] == 8]

    results = (missing_files, dark_files)

    return results


#    ALGORITHM 2
#    an algorithm with open dump files and a dictionary:
#    fast, faster than ALGORITHM 1, 6.5 min for DESY dumps
#    not suitable for big (>4GB) dumps
#    paths and statuses processed line by line

def consistency_check_faster(
    rucio_dump_before_path: str,
    rse_dump_path: str,
    rucio_dump_after_path: str,
    rucio_dump_parser: 'Callable' = lambda s: s,
    rse_dump_parser: 'Callable' = lambda s: s
) -> tuple[list[str], list[str]]:

    logger = logging.getLogger('auditorqt.consistencycheck.consistency_check_faster')
    logger.debug("Consistency check - faster")

    out = dict()

    file_rucio_dump_before = smart_open(rucio_dump_before_path)

    if file_rucio_dump_before is None:
        raise RuntimeError(f"Cannot open {rucio_dump_before_path}")

    # read and parse the dump line by line
    with file_rucio_dump_before:
        for line in file_rucio_dump_before:
            key, status = rucio_dump_parser(line)
            out[key] = 16
            if status == 'A':
                out[key] += 2

    file_rse_dump = smart_open(rse_dump_path)

    if file_rse_dump is None:
        raise RuntimeError(f"Cannot open {rse_dump_path}")

    with file_rse_dump:
        for line in file_rse_dump:
            key = rse_dump_parser(line)

            if key in out:
                out[key] += 8
            else:
                out[key] = 8

    file_rucio_dump_after = smart_open(rucio_dump_after_path)

    if file_rucio_dump_after is None:
        raise RuntimeError(f"Cannot open {rucio_dump_after_path}")

    # read and parse the dump line by line
    with file_rucio_dump_after:
        for line in file_rucio_dump_after:
            key, status = rucio_dump_parser(line)

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
    rucio_dump_parser: 'Callable' = lambda s: s,
    rse_dump_parser: 'Callable' = lambda s, p: s,
) -> Iterator[tuple[str, str]]:

    logger = logging.getLogger('auditorqt.consistencycheck.consistency_check_slow_reliable')
    logger.debug("Consistency check - slow, reliable")

    rucio_dump_before_path_parsed = parse_and_filter_file(
        rucio_dump_before_path,
        cache_dir=cache_dir,
        parser=lambda line: ','.join(rucio_dump_parser(line)),
    )

    rucio_dump_before_path_sorted = gnu_sort(
        rucio_dump_before_path_parsed,
        cache_dir=cache_dir,
        delimiter=',',
        fieldspec='1',
    )

    logger.debug("Rucio dump before sorted")

    rucio_dump_after_path_parsed = parse_and_filter_file(
        rucio_dump_after_path,
        cache_dir=cache_dir,
        parser=lambda line: ','.join(rucio_dump_parser(line)),
    )

    rucio_dump_after_path_sorted = gnu_sort(
        rucio_dump_after_path_parsed,
        cache_dir=cache_dir,
        delimiter=',',
        fieldspec='1',
    )

    logger.debug("Rucio dump after sorted")

    prefix_components = path_parsing_components(ddmendpoint_url(rse))

    rse_dump_path_parsed = parse_and_filter_file(
        rse_dump_path,
        cache_dir=cache_dir,
        parser=lambda line: rse_dump_parser(line, prefix_components),
    )

    rse_dump_path_sorted = gnu_sort(
        rse_dump_path_parsed,
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
