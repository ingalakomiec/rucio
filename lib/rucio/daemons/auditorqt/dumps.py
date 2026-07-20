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

"""perform actions on dumps after the auditor consistency check"""

import glob
import logging
import os


def remove_cached_dumps(paths: list[str]) -> bool:

    logging.getLogger('auditor: output.remove_cached_dump')

    for path in paths:
        # remove all dumps, also sorted and parsed
        remove = glob.glob(f"{path}*")
        for fil in remove:
            os.remove(fil)
    return True
