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

from __future__ import annotations

import logging

from rucio.common.dumper import http_download_to_file, smart_open, temp_file

CHUNK_SIZE = 4194304  # 4MiB


# FETCH #########################

# fetch
# used in profiles/atlas in fetch_rucio_dump
def download_rucio_dump(
    url: str,
    cache_dir: str,
    filename: str
) -> bool:

    with temp_file(cache_dir, final_name=filename) as (f, _):
        http_download_to_file(url, f)

    return True


# PREPARE #######################################

# prepare
# used as parser in concsistencycheck in ALGORITHM 2
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


# prepare
# used as parser in consistencycheck in ALGORITHM 3
def prepare_path_and_status_to_sort(line: str) -> str:

    path, status = parse_rucio_dump(line)

    return ','.join((path.strip(), status))


# prepare
# used as parser in consistencycheck in ALGORITHM 1
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
