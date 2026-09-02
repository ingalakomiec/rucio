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

"""generic rse dump fetching"""

from __future__ import annotations

import hashlib
import logging
import re
import shutil
from datetime import datetime


def fetch_rse_dump(
    source_path: str,
    rse: str,
    cache_dir: str,
    date: datetime | None = None,
    ) -> tuple[str, datetime]:

    logger = logging.getLogger('auditor.fetch_rse_dump')

    if date is None:
        date = datetime.now()

    # hash added to get a distinct file name
    hash = hashlib.sha1(source_path.encode()).hexdigest()
    filename = f"ddmendpoint_{rse}_{date:%d-%m-%Y}_{hash}"
    filename = re.sub(r'\W', '-', filename)
    final_path = f"{cache_dir}/{filename}"

    shutil.copyfile(source_path, final_path)

    logger.debug(f"RSE dump taken from: {source_path} and cached in: {final_path}")

    return (final_path, date)
