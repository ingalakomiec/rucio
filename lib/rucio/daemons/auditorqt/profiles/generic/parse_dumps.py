# GNU nano 7.2                                  ../algorithm3/parse_dumps.py
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

"""parse dumps for the consistency check"""

import logging

from rucio.daemons.auditorqt.consistencycheck.algorithm3.parse_dumps import path_parsing_components

# RUCIO DUMP PARSING #################


def parse_rucio_dump(line: str) -> tuple[str, str]:

    """
    Parse one line from Rucio replica dump.

    :param line: String with one line of a dump.
    :returns: (path, status)
    """

    logger = logging.getLogger('auditorqt.consistencycheck.parse_dumps.parse_rucio_dump')
    logger.debug("Parsing Rucio dump line")

    parts = line.strip().split()

    path = parts[7]
    status = parts[10]

    return path, status


# RSE DUMP PARSING #################

# for algorithm 1 and 2 in consistencycheck
def parse_rse_dump(
    line: str
) -> str:
    """
    Parse one line from an RSE dump.
    """
    logger = logging.getLogger('auditorqt.consistencycheck.parse_dumps.parse_rse_dump')
    logger.debug("Parsing RSE dump line")

    return line.strip()


# for algorithm 3 in consistencycheck
def parse_rse_dump_alg3(line: str, prefix_components: list[str]) -> str:
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


# funcions by parse_rse_dump_alg3
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
