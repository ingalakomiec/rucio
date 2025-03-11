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
#import logging
from typing import Optional

#from rucio.core.did import get_metadata
from rucio.common.dumper import DUMPS_CACHE_DIR

#from .generic import _call_for_attention, generic_move

#if TYPE_CHECKING:
#    from rucio.common.types import LoggerFunction

#    from .types import DecommissioningProfile

def atlas_auditor(
        rse: str,
        destdir: str = DUMPS_CACHE_DIR
#) -> tuple[str, datetime.datetime]:
):
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
    print(rse)
    #fetch RSE dump
 #   rse_dump = fetch_rse_dump()


    rucio_dump_before_path = '/opt/rucio/lib/rucio/daemons/auditorqt/tmp/real_dumps/rucio_dump_before/rucio_before.DESY-ZN_DATADISK_2025-01-24'

    rucio_dump_after_path = '/opt/rucio/lib/rucio/daemons/auditorqt/tmp/real_dumps/rucio_dump_after/rucio_after.DESY-ZN_DATADISK_2025-01-30'

    dest_dir = '/opt/rucio/lib/rucio/daemons/auditorqt/tmp/dump_file2'

    prepare_rucio_dump(rucio_dump_before_path, dest_dir)
    #fetch two rucio dumps - before and after
 #   rucio_dump_before = fetch_rucio_dump_before()
 #   rucio_dump_after = fetch_rucio_dump_after()

#    lost_files, dark_files = consistency_check(rucio_dump_before, rse_dump, rucio_dump_after)

    print("\nlost files")
#    print(lost_files)

    print("\ndark files")
#    print(dark_files)

#    file_lost_files = open('/opt/rucio/lib/rucio/daemons/auditorqt/tmp/lost_files', 'w')
#    file_lost_files.writelines(lost_files)
#    file_lost_files.close()

#    file_dark_files = open('/opt/rucio/lib/rucio/daemons/auditorqt/tmp/dark_files', 'w')
#    file_dark_files.writelines(dark_files)
#    file_dark_files.close()

    return True

def fetch_rse_dump():

    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!! fetching RSE dump")
# Collect all RSEs with the 'decommission' attribute
#    rses = get_rses_with_attribute(RseAttr.DECOMMISSION)
#    random.shuffle(rses)

#    file_rse_dump = open('/opt/rucio/lib/rucio/daemons/auditorqt/tmp/rse_dump', 'rt')
    file_rse_dump = open('/opt/rucio/lib/rucio/daemons/auditorqt/tmp/real_dumps/dump_20250127', 'rt')
    rse_dump = file_rse_dump.readlines()
    file_rse_dump.close()

    return rse_dump

def fetch_rucio_dump_before():

    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! fetching Rucio dump before")

#    file_rucio_dump_before = open('/opt/rucio/lib/rucio/daemons/auditorqt/tmp/rucio_dump_before', 'rt')
#    file_rucio_dump_before = open('/opt/rucio/lib/rucio/daemons/auditorqt/tmp/real_dumps/rucio_dump_before/rucio_before.DESY-ZN_DATADISK_2025-01-24', 'rt')

#    file_rucio_dump_after = open('/opt/rucio/lib/rucio/daemons/auditorqt/tmp/rucio_dump_after', 'rt')
#    file_rucio_dump_after = open('/opt/rucio/lib/rucio/daemons/auditorqt/tmp/real_dumps/rucio_dump_after/rucio_after.DESY-ZN_DATADISK_2025-01-30', 'rt')


#    rucio_dump_before = file_rucio_dump_before.readlines()
    rucio_dump_before = []
    with open('/opt/rucio/lib/rucio/daemons/auditorqt/tmp/real_dumps/rucio_dump_before/rucio_before.DESY-ZN_DATADISK_2025-01-24', 'rt') as file_rucio_dump_before:
        rucio_dump_before_tmp = file_rucio_dump_before.readlines()

        for line in rucio_dump_before_tmp:
            rucio_dump_before.append(line.strip().split()[7])

#    rucio_dump_before = file_rucio_dump_before.readlines()

    file_rucio_dump_before.close()

    return rucio_dump_before


def fetch_rucio_dump_after():

    print("!!!!!!!!!!!!!!!!!!!!!!!!! fetching Rucio dump after")

#    file_rucio_dump_before = open('/opt/rucio/lib/rucio/daemons/auditorqt/tmp/rucio_dump_before', 'rt')
#    file_rucio_dump_before = open('/opt/rucio/lib/rucio/daemons/auditorqt/tmp/real_dumps/rucio_dump_before/rucio_befor>

#    file_rucio_dump_after = open('/opt/rucio/lib/rucio/daemons/auditorqt/tmp/rucio_dump_after', 'rt')
#    file_rucio_dump_after = open('/opt/rucio/lib/rucio/daemons/auditorqt/tmp/real_dumps/rucio_dump_after/rucio_after.D>



#    rucio_dump_before = file_rucio_dump_before.readlines()

    rucio_dump_after = []
    with open('/opt/rucio/lib/rucio/daemons/auditorqt/tmp/real_dumps/rucio_dump_after/rucio_after.DESY-ZN_DATADISK_2025-01-30', 'rt') as file_rucio_dump_after:
        rucio_dump_after_tmp = file_rucio_dump_after.readlines()

        for line in rucio_dump_after_tmp:
            rucio_dump_after.append(line.strip().split()[7])

#    rucio_dump_after = file_rucio_dump_after.readlines()

    file_rucio_dump_after.close()

    return rucio_dump_after

def prepare_rucio_dump(
    dump_path: str,
    destdir: str
):

    rucio_dump = []
#    with open('/opt/rucio/lib/rucio/daemons/auditorqt/tmp/real_dumps/rucio_dump_after/rucio_after.DESY-ZN_DATADISK_2025-01-30', 'rt') as file_rucio_dump_after:
    with open(dump_path, 'rt') as file_rucio_dump:

        rucio_dump_tmp = file_rucio_dump.readlines()

        for line in rucio_dump_tmp:
            rucio_dump.append(line.strip().split()[7])
            rucio_dump.append('\n')

        file_rucio_dump.close()


    file_lost_files = open(destdir, 'w')
    file_lost_files.writelines(rucio_dump)
    file_lost_files.close()


    return True

def consistency_check(
    rucio_dump_before,
    rse_dump,
    rucio_dump_after):

    print("rucio_dump_before")
#    print(rucio_dump_before)

    print("\nrse dump")
#    print(rse_dump)

    print("\nrucio_dump_after")
#    print(rucio_dump_after)

    out = dict()

    for k in rucio_dump_before:
        out[k]=1

    for k in rse_dump:
        if k in out:
            out[k]+=2
        else:
            out[k]=2

    for k in rucio_dump_after:
        if k in out:
            out[k]+=4
        else:
            out[k]=4

    lost_files = [k for k in out if out[k]==5]
    dark_files = [k for k in out if out[k]==2]

    print("\nout")
#    print(out)
#    print(lost_files)
#    print(dark_files)

    results = (lost_files, dark_files)

    return results
