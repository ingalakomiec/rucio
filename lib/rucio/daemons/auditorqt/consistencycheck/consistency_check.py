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
import re
import subprocess  # noqa: S404 -- subprocess used for external commands
import tempfile
from collections.abc import Iterator
#from datetime import datetime
from typing import TYPE_CHECKING, Optional, Union, cast

from rucio.common.dumper import ddmendpoint_url, path_parsing, smart_open, temp_file

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator
    from datetime import datetime
    from _typeshed import SupportsNext
#    ALGORITHM 1
#    an algorithm with lists and a dictionary:
#    fast (7 min for DESY dumps),
#    not suitable for big (>4GB) dumps


def consistency_check_fast(
    rucio_dump_before_path: str,
    rse_dump_path: str,
    rucio_dump_after_path: str
) -> ([], []):

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
) -> [[], []]:

    logger = logging.getLogger('auditorqt.consistencycheck.consistency_check_faster')
    logger.debug("Consistency check - faster")

    out = dict()

    with smart_open(rucio_dump_before_path) as file_rucio_dump_before:

        for line in file_rucio_dump_before:
            parts = line.strip().split()
            # parts[7] - path
            # parts[10] - status (if available -> 'A')
            key = parts[7] + '\n'
            out[key] = 16
            if parts[10] == 'A':
                out[key] += 2

    with smart_open(rse_dump_path) as file_rse_dump:

        for line in file_rse_dump:
            if line in out:
                out[line] += 8
            else:
                out[line] = 8

    with smart_open(rucio_dump_after_path) as file_rucio_dump_after:

        for line in file_rucio_dump_after:
            parts = line.strip().split()
            # parts[7] - path
            # parts[10] - status (if available -> 'A')

            key = parts[7] + '\n'
            if key in out:
                out[key] += 4
                if parts[10] == 'A':
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
    results_path: str,
    rse: str,
    date: datetime,
    cache_dir: str
) -> None:

    results = slow_reliable_algorithm(
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
            status, path = result
            output.write(f"{status},{path}\n")

    return True


def rucio_dump_before_pathprepare_rse_dump(
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
) -> [[], []]:

    logger = logging.getLogger('auditorqt.consistencycheck.prepare_rucio_dump')
    logger.debug("Preparing Rucio dump")

    rucio_dump = [[], []]

    with smart_open(dump_path) as file_rucio_dump:

        for line in file_rucio_dump:
            # rucio_dump[0] - path
            rucio_dump[0].append(line.split()[7] + '\n')
            # rucio_dump[1] - status (if available -> 'A')
            rucio_dump[1].append(line.split()[10])

        file_rucio_dump.close()

    return rucio_dump


def parser(line: str) -> str:
    '''
    Simple parser for Rucio replica dumps.

    :param line: String with one line of a dump.
    :returns: A tuple with the path and status of the replica.
    '''
    fields = line.split('\t')
    path = fields[6].strip().lstrip('/')
    status = fields[8].strip()

    return ','.join((path, status))


def parse_and_filter_file(
        filepath: str,
        cache_dir: str,
        parser: 'Callable' = lambda s: s,
        filter_: 'Callable' = lambda s: s,
        prefix: Optional[str] = None,
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

    prefix = os.path.basename(filepath) if prefix is None else prefix
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


def gnu_sort(
        file_path: str,
        cache_dir: str,
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
        fieldspec: Optional[str] = None
) -> str:
    '''
    Sort the file with path `file_path` using the GNU sort command, the
    original file is unchanged, the output file is saved with path
    <cache_dir>/<prefix>_sorted.

    :param prefix: If given the output file will be named <prefix>_sorted.
    Otherwise the prefix is the name of the input file.
    :param delimiter: Delimiter character if the data is formatted in
    columns (argument of -t in the sort command).
    :param fieldspec: String with the specification of column or columns
    to be used to sort (argument -k in the sort command).
    :param cachedir: Working dir where the output file will be placed.

    Note: Using GNU sort to sort large files is convenient as it has low
    memory and it is relatively fast if used with the environment variable
    LC_ALL set to C as in this function.
    '''
    if (delimiter is not None) ^ (fieldspec is not None):
        raise ValueError("Either both delimiter and fieldspec is set, or neither are.")
    if delimiter is None:
        cmd_line = 'LC_ALL=C sort {0} > {1}'
    else:
        cmd_line = 'LC_ALL=C sort -t {0} -k {1} {{0}} > {{1}}'.format(delimiter, fieldspec)

    prefix = os.path.basename(file_path) if prefix is None else prefix

    sorted_name = '_'.join((prefix, 'sorted'))
    sorted_path = os.path.join(cache_dir, sorted_name)

    if os.path.exists(sorted_path):
        return sorted_path

    tfile = tempfile.NamedTemporaryFile(dir=cache_dir, delete=False)

    subprocess.check_call(
        cmd_line.format(file_path, tfile.name),
        shell=True,
    )

    os.link(tfile.name, sorted_path)
    os.unlink(tfile.name)

    return sorted_path


def strip_storage_dump(line: str, prefix_components) -> str:
    '''
    Parser to have consistent paths in storage dumps.

    :param line: String with one line of a dump.
    :returns: Path formatted as in the Rucio Replica Dumps.
    '''

    relative = path_parsing.remove_prefix(
        prefix_components,
        path_parsing.components(line),
    )
    if relative[0] == 'rucio':
        relative = relative[1:]
    return '/'.join(relative)


def slow_reliable_algorithm(
    rse: str,
    rse_dump_path: str,
    rucio_dump_before_path: str,
    rucio_dump_after_path: str,
    date: datetime,
    cache_dir: str
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

    standard_name_re = r'(ddmendpoint_{0}_\d{{2}}-\d{{2}}-\d{{4}}_[0-9a-f]{{40}})$'.format(rse)
    standard_name_match = re.search(standard_name_re, rse_dump_path)
    if standard_name_match is not None:
        # If the original filename was generated using the expected format,
        # just use the name as prefix for the parsed file.
        sd_prefix = standard_name_match.group(0)
    elif date is not None:
        # Otherwise try to use the date information and DDMEndpoint name to
        # have a meaningful filename.
        sd_prefix = 'ddmendpoint_{0}_{1}'.format(
            rse,
            date.strftime('%d-%m-%Y'),
        )
    else:
        # As last resort use only the DDMEndpoint name, but this is error
        # prone as old dumps may interfere with the checks.
        sd_prefix = 'ddmendpoint_{0}_unknown_date'.format(
            rse,
        )
        logger.warning(
            'Using basic and error prune naming for RSE dump as no date '
            'information was provided, %s dump will be named %s',
            rse,
            sd_prefix,
        )

    prefix_components = path_parsing.components(ddmendpoint_url(rse))
    rse_dump_path_sorted = gnu_sort(
        parse_and_filter_file(
            rse_dump_path,
            cache_dir=cache_dir,
            parser=lambda line: strip_storage_dump(line, prefix_components),
            prefix=sd_prefix,
        ),
        cache_dir=cache_dir,
        prefix=sd_prefix,
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


def compare3(
    it0: 'Iterable[str]',
    it1: 'Iterable[str]',
    it2: 'Iterable[str]'
) -> 'Iterator[tuple[str, tuple[bool, bool, bool], tuple[Optional[str], Optional[str]]]]':
    '''
    Generator to compare 3 sorted iterables, in each
    iteration it yields a tuple of the form (current, (bool, bool, bool))
    where current is the current element checked and the
    second element of the tuple is a triplet whose elements take
    a true value if current is contained in the it0, it1 or it2
    respectively.

    This function can't compare the iterators properly if None is
    a valid value.
    '''

    it0 = iter(it0)
    it1 = iter(it1)
    it2 = iter(it2)
    v0 = _try_to_advance(it0)
    v1 = _try_to_advance(it1)
    v2 = _try_to_advance(it2)

    while v0 is not None or v1 is not None or v2 is not None:
        path0, status0 = split_if_not_none(v0)
        path2, status2 = split_if_not_none(v2)

        vmin = min_value(path0, v1, path2)
        in0 = in1 = in2 = False
        in0_status = in2_status = None

        # Detect in which iterables the value is present
        #   inN is True if the value is present on the N iterable.
        #   sN  is the status of the path in the rucio replica
        #       dumps (N is either 0 or 2).
        if path0 is not None and path0 == vmin:
            in0 = True
            in0_status = status0

        if v1 is not None and v1 == vmin:
            in1 = True

        if path2 is not None and path2 == vmin:
            in2 = True
            in2_status = status2

        # yield the value, in which iterables is present, and the status
        # in each rucio replica dumps (if it is present there, else None).
        yield (vmin, (in0, in1, in2), (in0_status, in2_status))

        # Discard duplicate entries (it shouldn't be duplicate entries
        # anyways) and
        # advance the iterators, if the iterator N is depleted vN is set
        # to None.
        while v0 is not None and path0 == vmin:
            v0 = _try_to_advance(it0)
            path0, status0 = split_if_not_none(v0)

        while v1 is not None and v1 == vmin:
            v1 = _try_to_advance(it1)

        while v2 is not None and path2 == vmin:
            v2 = _try_to_advance(it2)
            path2, status2 = split_if_not_none(v2)


def _try_to_advance(
        it: 'SupportsNext[str]',
        default: Optional[str] = None
) -> Optional[str]:
    try:
        el = next(it)
    except StopIteration:
        return default
    return el.strip()


def split_if_not_none(
        value: Optional[str],
        sep: str = ',',
        fields: int = 2
) -> Union[str, list]:
    return value.split(sep) if value is not None else ([None] * fields)


def min_value(*values: Optional[str]) -> str:
    '''
    Minimum between the input values, ignoring None
    '''
    values_without_none = cast('list[str]', [value for value in values if value is not None])
    if len(values_without_none) == 0:
        raise ValueError("Input contains 0 non-null values.")
    return min(values_without_none)
