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

"""ATLAS-specific auditor profile."""

import glob
import hashlib
import logging
import os
import re
import requests

#from configparser import RawConfigParser
from datetime import datetime, timedelta
from typing import Any, Optional, Union

#from rucio.common.config import get_config_dirs
from rucio.common.constants import RseAttr
from rucio.core.rse import get_rse_id, list_rse_attributes

from rucio.daemons.auditorqt.profiles.atlas_specific.dumps import generate_url, fetch_object_store, fetch_no_object_store, download_rucio_dump
from rucio.daemons.auditorqt.profiles.atlas_specific.output import process_output

"""
_DUMPERCONFIGDIRS = list(
    filter(
        os.path.exists,
        (
            os.path.join(confdir, 'auditor') for confdir in get_config_dirs()
        )
    )
)


class Parser(RawConfigParser):
    '''
    RawConfigParser subclass that doesn't modify the the name of the options
    and removes any quotes around the string values.
    '''
    remove_quotes_re = re.compile(r"^'(.+)'$")
    remove_double_quotes_re = re.compile(r'^"(.+)"$')

    def optionxform(
            self,
            optionstr: str
    ) -> str:
        return optionstr

    def get(
            self,
            section: str,
            option: str
    ) -> Any:
        value = super(Parser, self).get(section, option)
        if isinstance(value, str):
            value = self.remove_quotes_re.sub(r'\1', value)
            value = self.remove_double_quotes_re.sub(r'\1', value)
        return value

    def items(self, section):
        return [(name, self.get(section, name)) for name in self.options(section)]
"""

def atlas_auditor(
        nprocs: int,
        rse: str,
        keep_dumps: bool,
        delta: timedelta,
        cache_dir: str,
        results_dir: str
) -> Optional[str]:
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

    logger = logging.getLogger('atlas_auditor')

    date = datetime.today()
    delta = timedelta(delta)

    rse_dump_path = '/opt/rucio/lib/rucio/daemons/auditorqt/tmp/real_dumps/dump_20250127'
    rucio_dump_before_path = '/opt/rucio/lib/rucio/daemons/auditorqt/tmp/real_dumps/rucio_dump_before/rucio_before.DESY-ZN_DATADISK_2025-01-24'
    rucio_dump_after_path = '/opt/rucio/lib/rucio/daemons/auditorqt/tmp/real_dumps/rucio_dump_after/rucio_after.DESY-ZN_DATADISK_2025-01-30'

#    configuration = parse_configuration()
#    rse_dump_path_tmp, date_rse = fetch_rse_dump(rse, configuration, cache_dir, date)


    rse_dump_path_tmp, date_rse = fetch_rse_dump(rse, cache_dir, date)

    rucio_dump_before_path_tmp = fetch_rucio_dump(rse, date - delta, cache_dir)
    rucio_dump_after_path_tmp = fetch_rucio_dump(rse, date_rse + delta, cache_dir)



    result_file_name = f"result.{rse}_{date:%Y%m%d}"
    results_path = f"{results_dir}/{result_file_name}"



    if os.path.exists(f"{results_path}") or os.path.exists(f"{results_path}.bz2"):
        logger.warning(f"Consistency check for {rse}, dump dated {date:%d-%m-%Y}, already done. Skipping consistency check.")
        return results_path


    lost_files, dark_files = consistency_check(rucio_dump_before_path, rse_dump_path, rucio_dump_after_path)

    file_results = open(results_path, 'w')

    for k in range(len(dark_files)):
        file_results.write('DARK'+(dark_files[k]).replace("/",",",1))

    for k in range(len(lost_files)):
        file_results.write('LOST'+(lost_files[k]).replace("/",",",1))

    file_results.close()

    process_output(rse, results_path, sanity_check = False)

    return results_path

"""
def parse_configuration(conf_dirs: Optional[list[str]] = None) -> Parser:

    conf_dirs = conf_dirs or _DUMPERCONFIGDIRS
    logger = logging.getLogger('auditor.parse_configuration')

    if len(conf_dirs) == 0:
        logger.error('No configuration directory given to load RSE dump path')
        raise Exeption('No configuration directory given to load RSE dump path')
        print("conf_dirs: ", conf_dirs)

    configuration = Parser({
        'disabled': False,
    })

    for conf_dir in conf_dirs:
        configuration.read(glob.glob(conf_dir + '/*.cfg'))

    return configuration
"""

def fetch_rse_dump(
    rse: str,
#    configuration: RawConfigParser,
    cache_dir: str,
    date: Optional[datetime] = None,
) -> tuple[str, datetime]:

    logger = logging.getLogger('auditor.fetch_rse_dump')

#    base_url, url_pattern = generate_url(rse, configuration)
    base_url = generate_url(rse)

    rse_id = get_rse_id(rse)
    rse_attr = list_rse_attributes(rse_id)

    if RseAttr.IS_OBJECT_STORE in rse_attr and rse_attr[RseAttr.IS_OBJECT_STORE] is not False:
        path, date = fetch_object_store(rse, base_url, cache_dir, date)

    else:
        path, date = fetch_no_object_store(rse, base_url, cache_dir, date)

    return (path, date)

def fetch_rucio_dump(
    rse: str,
    date: "datetime",
    cache_dir: str
) -> str:

    logger = logging.getLogger('auditor.fetch_rucio_dump')


#    url = 'https://eosatlas.cern.ch//eos/atlas/atlascerngroupdisk/data-adc/rucio-analytix/reports/2025-05-04/replicas_per_rse/GOEGRID_TESTDATADISK.replicas_per_rse.2025-05-04.csv.bz2'
    url = get_rucio_dump_url(date, rse)
    # the line below just for tests
    url = 'https://learnpython.com/blog/python-pillow-module/1.jpg'

    # hash added to create a unic filename
    hash = hashlib.sha1(url.encode()).hexdigest()
    filename = f"{rse}_{date:%Y-%m-%d}_{hash}"
    filename = re.sub(r'\W', '-', filename)
    path = f"{cache_dir}/{filename}"

    if not os.path.exists(path):
        logging.debug('Trying to download: %s for %s', url, rse)
        download_rucio_dump(url, cache_dir, filename)
    else:
        logger.debug('Taking Rucio Replica Dump %s for %s from cache', path, rse)

    return path

def get_rucio_dump_url(
    date: datetime,
    rse: str
) -> str:

    url  = f"https://eosatlas.cern.ch/eos/atlas/atlascerngroupdisk/data-adc/rucio-analytix/reports/{date:%Y-%m-%d}/replicas_per_rse/{rse}.replicas_per_rse.{date:%Y-%m-%d}.csv.bz2"
    return url

def prepare_rse_dump(
    dump_path: str
) -> []:

    print("preparing rse dump")

    file_rse_dump = open(dump_path, 'rt')
    rse_dump = file_rse_dump.readlines()
    file_rse_dump.close()

    return rse_dump


def prepare_rucio_dump(
    dump_path: str
) -> [[],[]]:

    print("preparing rucio dump")

    rucio_dump = [[],[]]

    with open(dump_path, 'rt') as file_rucio_dump:

        for line in file_rucio_dump:
            rucio_dump[0].append(line.split()[7]+'\n')
            rucio_dump[1].append(line.split()[10])

        file_rucio_dump.close()


    return rucio_dump

def consistency_check(
    rucio_dump_before_path: str,
    rse_dump_path: str,
    rucio_dump_after_path: str
) -> ([],[]):

    print("consistency check")


    rucio_dump_before = prepare_rucio_dump(rucio_dump_before_path)

    out = dict()

    i = 0

    for k in rucio_dump_before[0]:
        out[k]=16
        if rucio_dump_before[1][i]=='A':
            out[k]+=2
        i+=1

    del rucio_dump_before

    rse_dump = prepare_rse_dump(rse_dump_path)


    i = 0
    for k in rse_dump:
        if k in out:
            out[k]+=8
        else:
            out[k]=8

    del rse_dump

    rucio_dump_after = prepare_rucio_dump(rucio_dump_after_path)

    for k in rucio_dump_after[0]:
        if k in out:
            out[k]+=4
            if rucio_dump_after[1][i]=='A':
                out[k]+=1
        else:
            out[k]=4
        i+=1

    del rucio_dump_after

    lost_files = [k for k in out if out[k]==23]
    dark_files = [k for k in out if out[k]==8]

    results = (lost_files, dark_files)

    return results
