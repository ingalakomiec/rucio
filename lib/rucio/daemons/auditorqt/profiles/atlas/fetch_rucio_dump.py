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

"""ATLAS-specific rucio dump fetching"""

from __future__ import annotations

import hashlib
import logging
import os
import re
from typing import TYPE_CHECKING

from rucio.common.dumper import http_download_to_file, temp_file

if TYPE_CHECKING:
    from datetime import datetime


def fetch_rucio_dump(
    rse: str,
    date: datetime,
    cache_dir: str
) -> str:

    logger = logging.getLogger('auditor.fetch_rucio_dump')

    url = get_rucio_dump_url(date, rse)

    # two lines below just for tests
    # url = 'https://eosatlas.cern.ch//eos/atlas/atlascerngroupdisk/data-adc/rucio-analytix/reports/2025-05-04/replicas>
    url = "https://learnpython.com/blog/python-pillow-module/1.jpg"

    # hash added to create a unique filename
    hash = hashlib.sha1(url.encode()).hexdigest()
    filename = f"{rse}_{date:%Y-%m-%d}_{hash}"
    filename = re.sub(r'\W', '-', filename)
    path = f"{cache_dir}/{filename}"

    if not os.path.exists(path):
        logging.debug(f"Trying to download: {url} for {rse}")
        download_rucio_dump(url, cache_dir, filename)
    else:
        logger.debug(f"Taking Rucio Replica Dump {path} for {rse} from cache")

    return path


# used here in fetch_rucio_dump
def get_rucio_dump_url(
    date: datetime,
    rse: str
) -> str:

    url = f"https://eosatlas.cern.ch/eos/atlas/atlascerngroupdisk/data-adc/rucio-analytix/reports/{date:%Y-%m-%d}/replicas_per_rse/{rse}.replicas_per_rse.{date:%Y-%m-%d}.csv.bz2"

    return url


# used here in fetch_rucio_dump
def download_rucio_dump(
    url: str,
    cache_dir: str,
    filename: str
) -> bool:

    with temp_file(cache_dir, final_name=filename) as (f, _):
        http_download_to_file(url, f)

    return True
