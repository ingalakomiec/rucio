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

"""ATLAS-specific auditor profiles."""

import datetime
from typing import Optional

from rucio.common.dumper import DUMPS_CACHE_DIR

def atlas_auditor(
        rse: str,
        destdir: str = DUMPS_CACHE_DIR
) -> None:
    '''
    Downloads the dump for the given ddmendpoint. If this endpoint does not
    follow the standardized method to publish the dumps it should have an
    entry in the `configuration` object describing how to download the dump.

    `rse` is the DDMEndpoint name.

    `configuration` is a RawConfigParser subclass.

    `date` is a datetime instance with the date of the desired dump or None
    to download the latest available dump.

    `destdir` is the directory where the dump will be saved (the final component
    in the path is created if it doesn't exist).

    Return value: a tuple with the filename and a datetime instance with
    the date of the dump.
    '''

    rse_dump_path = '/opt/rucio/lib/rucio/daemons/auditorqt/tmp/real_dumps/dump_20250127'

    rucio_dump_before_path = '/opt/rucio/lib/rucio/daemons/auditorqt/tmp/real_dumps/rucio_dump_before/rucio_before.DESY-ZN_DATADISK_2025-01-24'

    rucio_dump_after_path = '/opt/rucio/lib/rucio/daemons/auditorqt/tmp/real_dumps/rucio_dump_after/rucio_after.DESY-ZN_DATADISK_2025-01-30'

    rse_dump = fetch_rse_dump(rse_dump_path)

    rucio_dump_before = fetch_rucio_dump(rucio_dump_before_path)
    rucio_dump_after = fetch_rucio_dump(rucio_dump_after_path)

    lost_files, dark_files = consistency_check(rucio_dump_before, rse_dump, rucio_dump_after)

    file_lost_files = open('/opt/rucio/lib/rucio/daemons/auditorqt/tmp/lost_files', 'w')
    file_lost_files.writelines(lost_files)
    file_lost_files.close()

    file_dark_files = open('/opt/rucio/lib/rucio/daemons/auditorqt/tmp/dark_files', 'w')
    file_dark_files.writelines(dark_files)
    file_dark_files.close()

    return True

def fetch_rse_dump(
    dump_path: str
) -> []:

    print("fetching rse dump")

    file_rse_dump = open(dump_path, 'rt')
    rse_dump = file_rse_dump.readlines()
    file_rse_dump.close()

    return rse_dump


def fetch_rucio_dump(
    dump_path: str
) -> [[],[]]:

    print("fetching rucio dump")

    rucio_dump = [[],[]]

    with open(dump_path, 'rt') as file_rucio_dump:

        for line in file_rucio_dump:
            rucio_dump[0].append(line.split()[7]+'\n')
            rucio_dump[1].append(line.split()[10])

        file_rucio_dump.close()


    return rucio_dump

def consistency_check(
    rucio_dump_before: [[],[]],
    rse_dump: [],
    rucio_dump_after: [[],[]]
) -> ([],[]):

    print("consistency check")

    out = dict()

    i = 0

    for k in rucio_dump_before[0]:
        out[k]=16
        if rucio_dump_before[1][i]=='A':
            out[k]+=2
        i+=1

    i = 0
    for k in rse_dump:
        if k in out:
            out[k]+=8
        else:
            out[k]=8


    for k in rucio_dump_after[0]:
        if k in out:
            out[k]+=4
            if rucio_dump_after[1][i]=='A':
                out[k]+=1
        else:
            out[k]=4
        i+=1

    lost_files = [k for k in out if out[k]==23]
    dark_files = [k for k in out if out[k]==8]

    results = (lost_files, dark_files)

    return results
