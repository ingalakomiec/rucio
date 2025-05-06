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

import logging
import os
import requests
import urllib.request

from datetime import datetime, timedelta
from typing import Optional, Union

BASE_URL = 'https://eosatlas.cern.ch/eos/atlas/atlascerngroupdisk/data-adc/rucio-analytix/reports/{0}/replicas_per_rse/{1}*'
#BASE_URL = '/user/rucio01/reports/{0}/replicas_per_rse/{1}'

def atlas_auditor(
        nprocs: int,
        rse: str,
        keep_dumps: bool,
        delta: timedelta,
        cache_dir: str,
        results_dir: str
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

    date = datetime.now()
    delta = timedelta(delta)

    rse_dump_path = '/opt/rucio/lib/rucio/daemons/auditorqt/tmp/real_dumps/dump_20250127'

    rucio_dump_before_path = '/opt/rucio/lib/rucio/daemons/auditorqt/tmp/real_dumps/rucio_dump_before/rucio_before.DESY-ZN_DATADISK_2025-01-24'

    rucio_dump_after_path = '/opt/rucio/lib/rucio/daemons/auditorqt/tmp/real_dumps/rucio_dump_after/rucio_after.DESY-ZN_DATADISK_2025-01-30'

    rse_dump_before_path_tmp = fetch_rucio_dump(rse, date - delta, cache_dir)
#    rse_dump_after_path_tmp = fetch_rucio_dump(rse, date + delta, cache_dir)

    lost_files, dark_files = consistency_check(rucio_dump_before_path, rse_dump_path, rucio_dump_after_path)

    file_lost_files = open(results_dir+'/lost_files', 'w')
    file_lost_files.writelines(lost_files)
    file_lost_files.close()

    file_dark_files = open(results_dir+'/dark_files', 'w')
    file_dark_files.writelines(dark_files)
    file_dark_files.close()

    result_file_name = 'result.{0}_{1}'.format(
        rse,
        date.strftime('%Y%m%d')
    )

    results_path = os.path.join(results_dir, result_file_name)

    file_results = open(results_path, 'w')

    for k in range(len(dark_files)):
        file_results.write('DARK'+(dark_files[k]).replace("/",",",1))

    for k in range(len(lost_files)):
        file_results.write('LOST'+(lost_files[k]).replace("/",",",1))

    file_results.close()

    return True

def fetch_rse_dump(
    rse: str
) -> None:

    return True

def fetch_rucio_dump(
    rse: str,
    date: "datetime",
    cache_dir: str
) -> str:

    logger = logging.getLogger('auditor.fetch_rucio_dump')
    print("fetching rucio dump for rse: "+rse)

    date = date.strftime('%Y-%m-%d')

#    url = BASE_URL.format(date,rse)
#    url = ''.join((BASE_URL, '?date={0}&rse={1}'.format(date, rse)))
#    url = BASE_URL.format(date, rse)
    url = 'https://eosatlas.cern.ch//eos/atlas/atlascerngroupdisk/data-adc/rucio-analytix/reports/2025-05-04/replicas_per_rse/GOEGRID_TESTDATADISK.replicas_per_rse.2025-05-04.csv.bz2'

#    url = 'https://learnpython.com/blog/python-pillow-module/1.jpg'

#    url = 'http://google.com/favicon.ico'

    print('url:', url)

    filename = '{0}_{1}'.format(
        rse,
        date
    )

#    filename = re.sub(r'\W', '-', filename)
    path = os.path.join(cache_dir, filename)


# te trzy linijki poten odkomentowac
#    if os.path.exists(path):
#        logger.debug('Taking Rucio Replica Dump %s for %s from cache', path, rse)
#        return path

    logging.debug('Trying to download: %s for %s', url, rse)

#    response = requests.get(url, allow_redirects=True, stream=True)

#    response = requests.get(url, allow_redirects=True)

#    if response.status_code != 200:
#        logging.error(
#        'Retrieving %s returned %d status code',
#        url,
#        response.status_code,
#        )

#    open('temporary.jpg', 'wb').write(response.content)

    urllib.request.urlretrieve(url, "/opt/rucio/lib/rucio/daemons/auditorqt/tmp/file_tmp.zip")

    return path

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

#    rucio_dump_before = prepare_rucio_dump(rucio_dump_before_path)


    out = dict()
    """
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
    """
    lost_files = [k for k in out if out[k]==23]
    dark_files = [k for k in out if out[k]==8]

    results = (lost_files, dark_files)

    return results
