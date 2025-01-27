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

"""Auditor profile definitions."""

from .atlas import atlas_auditor
from .atlasOld import atlas_auditor_old
from .generic import generic_auditor

PROFILE_MAP = {
#    'generic_delete': generic_delete,
#    'generic_move': generic_move,
    'atlas': atlas_auditor,
    'atlasOld': atlas_auditor_old,
    'generic': generic_auditor
}
