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

"""perform actions on dumps needed before and after the auditor consistency check"""

from __future__ import annotations

import glob
import logging
import os
import subprocess  # noqa: S404 -- subprocess used for external commands
import tempfile
from typing import TYPE_CHECKING

from rucio.common.dumper import smart_open, temp_file

if TYPE_CHECKING:
    from collections.abc import Callable


def gnu_sort(
        file_path: str,
        cache_dir: str,
        prefix: str | None = None,
        delimiter: str | None = None,
        fieldspec: str | None = None
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


def remove_cached_dumps(paths: list[str]) -> bool:

    logging.getLogger('auditor: output.remove_cached_dump')

    for path in paths:
        # remove all dumps, also sorted and parsed
        remove = glob.glob(f"{path}*")
        for fil in remove:
            os.remove(fil)
    return True


def path_parsing_remove_prefix(prefix: list[str], path: list[str]) -> list[str]:
    """
    Remove the specified prefix from the given path.

    :param prefix: The prefix to be removed from the path.
    :param path: The path from which the prefix should be removed.

    :return: The path with the prefix removed.
            If the prefix is not found at the start of the path, the original path is returned.
            If the path is a subset of the prefix, an empty list is returned.
    """

    iprefix = iter(prefix)
    ipath = iter(path)
    try:
        cprefix = next(iprefix)
        cpath = next(ipath)
    except StopIteration:
        # Either the path or the prefix is empty
        return path
    while cprefix != cpath:
        try:
            cprefix = next(iprefix)
        except StopIteration:
            # No parts of the prefix are part of the path
            return path

    while cprefix == cpath:
        cprefix = next(iprefix, None)
        try:
            cpath = next(ipath)
        except StopIteration:
            # The path is a subset of the prefix
            return []

    if cprefix is not None:
        # If the prefix is not depleted maybe it is only a coincidence
        # in one of the components of the paths: return the path as is.
        return path

    rest = list(ipath)
    rest.insert(0, cpath)
    return rest


def path_parsing_components(path: str) -> list[str]:
    """
    Extracts and returns the non-empty components of a given path.

    :param path: input path string to be parsed.

    :return: list of non-empty components of the path.
    """

    components = path.strip().strip().split()
    return [component for component in components if component != '']


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
