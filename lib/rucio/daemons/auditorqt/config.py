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

# config for auditorqt

import os
from configparser import NoSectionError

from rucio.common.config import config_get, config_has_section


def get_auditor_cache_directory() -> str:
    if not config_has_section('auditor'):
        raise NoSectionError("Auditor section required in config tu run te auditor daemon.")

    cache_dir = config_get('auditor', 'cache')

    os.makedirs(cache_dir, exist_ok=True)

    return cache_dir


def get_auditor_results_directory() -> str:
    if not config_has_section('auditor'):
        raise NoSectionError("Auditor section required in config tu run te auditor daemon.")

    results_dir = config_get('auditor', 'results')

    os.makedirs(results_dir, exist_ok=True)

    return results_dir
