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

import logging

from rucio.common.dumper import smart_open


# used in consistencycheck in ALGORITHM 1
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
