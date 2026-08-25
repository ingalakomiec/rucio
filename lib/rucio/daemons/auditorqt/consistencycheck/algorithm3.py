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

"""compare 3 lists (for ALGORITHM 3 from the consistency check)"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from _typeshed import SupportsNext
    from collection.abc import Iterable, Iterator


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


def _try_to_advance(
        it: 'SupportsNext[str]',
        default: str | None = None
) -> str | None:
    try:
        el = next(it)
    except StopIteration:
        return default
    return el.strip()


def min_value(*values: str | None) -> str:
    '''
    Minimum between the input values, ignoring None
    '''
    values_without_none = cast('list[str]', [value for value in values if value is not None])
    if len(values_without_none) == 0:
        raise ValueError("Input contains 0 non-null values.")
    return min(values_without_none)


def split_if_not_none(
        value: str | None,
        sep: str = ',',
        fields: int = 2
) -> str | list:
    return value.split(sep) if value is not None else ([None] * fields)
