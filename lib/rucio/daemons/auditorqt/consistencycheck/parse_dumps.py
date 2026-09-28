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

"""parse dumps for the consistency check algorithm1"""

# used as parser in concsistencycheck in ALGORITHM 2, used in ALG 1 and 3


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
