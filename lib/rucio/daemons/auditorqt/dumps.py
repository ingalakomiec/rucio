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
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from _typeshed import SupportsNext


def compare3(
    it0: 'Iterable[str]',
    it1: 'Iterable[str]',
    it2: 'Iterable[str]'
) -> 'Iterator[tuple[str, tuple[bool, bool, bool], tuple[str | None, str | None]]]':
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


def _try_to_advance(
        it: 'SupportsNext[str]',
        default: str | None = None
) -> str | None:
    try:
        el = next(it)
    except StopIteration:
        return default
    return el.strip()


def split_if_not_none(
        value: str | None,
        sep: str = ',',
        fields: int = 2
) -> str | list:
    return value.split(sep) if value is not None else ([None] * fields)


def min_value(*values: str | None) -> str:
    '''
    Minimum between the input values, ignoring None
    '''
    values_without_none = cast('list[str]', [value for value in values if value is not None])
    if len(values_without_none) == 0:
        raise ValueError("Input contains 0 non-null values.")
    return min(values_without_none)
