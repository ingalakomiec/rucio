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

"""parse dumps for algorithm3 in consistency check"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from rucio.common.dumper import smart_open, temp_file

if TYPE_CHECKING:
    from collections.abc import Callable


# used in consistencycheck in ALGORITHM 3
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


def path_parsing_components(path: str) -> list[str]:
    """
    Extracts and returns the non-empty components of a given path.

    :param path: input path string to be parsed.

    :return: list of non-empty components of the path.
    """

    components = path.strip().strip().split()
    return [component for component in components if component != '']
