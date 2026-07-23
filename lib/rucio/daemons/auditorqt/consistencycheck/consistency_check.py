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
import os
from typing import TYPE_CHECKING

from rucio.common.dumper import ddmendpoint_url, smart_open, temp_file
from rucio.daemons.auditorqt.dumps import compare3, gnu_sort, path_parsing_components, path_parsing_remove_prefix

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

#    ALGORITHM 1
#    an algorithm with lists and a dictionary:
#    fast (7 min for DESY dumps),
#    not suitable for big (>4GB) dumps


def consistency_check_fast(
    rucio_dump_before_path: str,
    rse_dump_path: str,
    rucio_dump_after_path: str
) -> tuple[list[str], list[str]]:

    logger = logging.getLogger('auditorqt.consistencycheck.consistency_check_fast')
    logger.debug("Consistency check - fast")

    rucio_dump_before = prepare_rucio_dump(rucio_dump_before_path)

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

    rucio_dump_after = prepare_rucio_dump(rucio_dump_after_path)

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
    rucio_dump_after_path: str
) -> tuple[list[str], list[str]]:

    logger = logging.getLogger('auditorqt.consistencycheck.consistency_check_faster')
    logger.debug("Consistency check - faster")

    out = dict()

    file_rucio_dump_before = smart_open(rucio_dump_before_path)

    if file_rucio_dump_before is None:
        raise RuntimeError(f"Cannot open {rucio_dump_before_path}")

    with file_rucio_dump_before:
        for line in file_rucio_dump_before:
            key, status = parse_rucio_dump(line)
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
            key, status = parse_rucio_dump(line)

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
    cache_dir: str
) -> Iterator[tuple[str, str]]:

    logger = logging.getLogger('auditorqt.consistencycheck.consistency_check_slow_reliable')
    logger.debug("Consistency check - slow, reliable")

    rucio_dump_before_path_sorted = gnu_sort(
        parse_and_filter_file(rucio_dump_before_path, cache_dir=cache_dir, parser=prepare_path_and_status_to_sort),
        cache_dir=cache_dir,
        delimiter=',',
        fieldspec='1',
    )

    logger.debug("Rucio dump before sorted")

    rucio_dump_after_path_sorted = gnu_sort(
        parse_and_filter_file(rucio_dump_after_path, cache_dir=cache_dir, parser=prepare_path_and_status_to_sort),
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


def prepare_rse_dump(
    dump_path: str
) -> list[str]:

    logger = logging.getLogger('auditorqt.consistencycheck.prepare_rse_dump')
    logger.debug("Preparing RSE dump")

    file_rse_dump = smart_open(dump_path)

    if file_rse_dump is None:
        raise RuntimeError(f"Cannot open {dump_path}")

    rse_dump = [line.strip() for line in file_rse_dump]
    file_rse_dump.close()

    return rse_dump


def prepare_rucio_dump(
    dump_path: str
) -> tuple[list[str], list[str]]:

    logger = logging.getLogger('auditorqt.consistencycheck.prepare_rucio_dump')
    logger.debug("Preparing Rucio dump")

    paths = []
    statuses = []

    file_rucio_dump = smart_open(dump_path)

    if file_rucio_dump is None:
        raise RuntimeError(f"Cannot open {dump_path}")

    with file_rucio_dump:
        for line in file_rucio_dump:
            path, status = parse_rucio_dump(line)
            paths.append(path)
            statuses.append(status)

    return paths, statuses


def parse_rucio_dump(line: str) -> tuple[str, str]:
    '''
    Parse one line from Rucio replica dump.

    :param line: String with one line of a dump.
    :returns: (path, status)
    '''

    parts = line.strip().split()

    path = parts[7]
    status = parts[10]

    return path, status


def prepare_path_and_status_to_sort(line: str) -> str:

    path, status = parse_rucio_dump(line)

    return ','.join((path.strip(), status))


def parse_and_filter_file(
        filepath: str,
        cache_dir: str,
        parser: 'Callable' = lambda s: s,
        filter_: 'Callable' = lambda s: s,
        postfix: str = 'parsed'
) -> str:
    '''
    Opens `filepath` as a read-only file, and for each line of the file
    for which the `filter_` function returns True, it writes a version
    parsed with the `parser` function.

    The name of the output file is generated appending '_' + `postfix` to
    the filename in `filepath`. If `prefix` is given it is used instead
    of `filepath`.

    The output file (and temporary files while processing are stored in
    `cache_dir`.

    Default values for the arguments:
        - `parser`: returns the same string.
        - `filter_`: returns True for any argument.
        - `prefix`: None (the name of the input file is used as prefix).
        - `postfix`: 'parsed'.
        - `cache_dir`: DUMPS_CACHE_DIR.

    The output file is created with a random name and renamed atomically
    when it is complete.

    '\n' is appended to each line, therefore if the input is 'a\nb\n' and `parser`
    is not especified the output will be 'a\n\nb\n\n'
    '''

    prefix = os.path.basename(filepath)
    output_name = '_'.join((prefix, postfix))
    output_path = os.path.join(cache_dir, output_name)

    if os.path.exists(output_path):
        return output_path

    with temp_file(cache_dir, final_name=output_name) as (output, _):
        input_ = smart_open(filepath)
        if input_ is not None:
            for line in input_:
                if filter_(line):
                    output.write(parser(line) + '\n')

            input_.close()

    return output_path


def parse_rse_dump(line: str, prefix_components: list[str]) -> str:
    '''
    Parser to have consistent paths in storage dumps.

    :param line: String with one line of a dump.
    :returns: Path formatted as in the Rucio Replica Dumps.
    '''

    relative = path_parsing_remove_prefix(
        prefix_components,
        path_parsing_components(line),
    )
    if relative[0] == 'rucio':
        relative = relative[1:]
    return '/'.join(relative)
