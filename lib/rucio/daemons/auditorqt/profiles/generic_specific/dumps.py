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

"""action on RSE and Rucio dumps: fetching, removing cached dumps"""

import logging

from rucio.common.dumper import smart_open

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
