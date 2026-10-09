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

"""sort dumps for algorithm3 in consistency check"""

from __future__ import annotations

import os
import subprocess  # noqa: S404 -- subprocess used for external commands
import tempfile


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
