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

"""perform actions on output of the auditor consistency check"""

import bz2
import logging
import os

from typing import Optional

from rucio.common import config
from rucio.common.types import InternalAccount, InternalScope
from rucio.common.utils import chunks
from rucio.core.quarantined_replica import add_quarantined_replicas
from rucio.core.replica import declare_bad_file_replicas, list_replicas
from rucio.core.rse import get_rse_id, get_rse_usage
from rucio.db.sqla.constants import BadFilesStatus


def process_output(
    rse: str,
    results_path: str,
    sanity_check: bool = True,
    compress: bool = True
) -> None:

    """Perform post-consistency-check actions.

    DARK files are put in the quarantined-replica table so that they
    may be deleted by the Dark Reaper. Missed files are reported as
    suspicious so that they may be further checked by the cloud squads.

    ``results_path``: absolute path to the file
    produced by ``consistency_check()``.

    If ``sanity_check`` is ``True`` (default) and the number of entries
    in the output file is deemed excessive, the actions are aborted.

    If ``compress`` is ``True`` (default), the file is compressed with
    bzip2 after the actions are successfully performed.
    """

    logger = logging.getLogger('atlas_auditor output.process_output')

    dark_replicas = []
    missing_replicas = []
    try:
        with open(results_path) as f:
            for line in f:
                label, path = line.rstrip().split(',', 1)
                scope, name = guess_replica_info(path)

                if label == 'DARK':
                    dark_replicas.append({'path': path,
                                          'scope': InternalScope(scope),
                                          'name': name})
                elif label == 'MISSING':
                    missing_replicas.append({'scope': InternalScope(scope),
                                          'name': name})
                else:
                    raise ValueError('unexpected label')

   # Since the file is read immediately after its creation, any error
   # exposes a bug in the Auditor.
    except Exception as error:
        logger.critical(f"Error processing {results_path}", exc_info=True)
        raise error

    rse_id = get_rse_id(rse=rse)
    usage = get_rse_usage(rse_id=rse_id, source='rucio')[0]
    threshold = config.config_get_float('auditor', 'threshold', False, 0.1)

    # Perform a basic sanity check by comparing the number of entries
    # with the total number of files on the RSE.  If the percentage is
    # significant, there is most likely an issue with the site dump.
    found_error = False

    if len(dark_replicas) > threshold * usage['files']:
        logger.warning(f"Number of DARK files is exceeding threshold: {results_path}")
        found_error = True

    if len(missing_replicas) > threshold * usage['files']:
        logger.warning(f"Number of MISSING files is exceeding threshold: {results_path}")
        found_error = True

    if found_error and sanity_check:
        raise AssertionError("sanity check failed")

    # While converting MISSING replicas to PFNs, entries that do not
    # correspond to a replica registered in Rucio are silently dropped.

    missed_pfns = [r['rses'][rse_id][0] for chunk in chunks(missed_replicas, 1000) for r in list_replicas(chunk) if rse_id in r['rses']]

    for chunk in chunks(dark_replicas, 1000):
        add_quarantined_replicas(rse_id=rse_id, replicas=chunk)

    logger.debug(f"Processed {len(dark_replicas)} DARK files from {results_path}")

    declare_bad_file_replicas(missed_pfns, reason='Reported by Auditor',
                              issuer=InternalAccount('root'), status=BadFilesStatus.SUSPICIOUS)

    logger.debug(f"Processed {len(missing_replicas)} MISSING files from {results_path}")

    if compress:
        final_path = bz2_compress_file(results_path)
        logger.debug(f"Compressed {final_path}")

    return True

def bz2_compress_file(
        source_path: str,
        chunk_size: int = 65000
) -> str:

    """Compress a file with bzip2.

    The destination is the path passed through ``source`` extended with
    '.bz2'.  The original file is deleted.

    Errors are deliberately not handled gracefully.  Any exceptions
    should be propagated to the caller.

    ``source_path``: absolute path to the file to compress.

    ``chunk_size``: size (in bytes) of the chunks by which to read the file.

    Returns the destination path.
    """

    final_path = f"{source_path}.bz2"
    with open(source_path) as plain, bz2.BZ2File(final_path, 'w') as compressed:
        while True:
            chunk = plain.read(chunk_size)
            if not chunk:
                break
            compressed.write(chunk.encode())
    os.remove(source_path)
    return final_path


def guess_replica_info(
    path: str
) -> tuple[Optional[str], str]:


    """Try to extract the scope and name from a path.

    ``path``: relative path to the file on the RSE.

    Returns a ``tuple`` of which the first element is the scope of the
    replica and the second element is its name.
    """

    items = path.split('/')
    if len(items) == 1:
        return None, path
    elif len(items) > 2 and items[0] in ['group', 'user']:
        return '.'.join(items[0:2]), items[-1]
    else:
        return items[0], items[-1]

def remove_cached_dumps(paths: []) -> bool:

    logger = logging.getLogger('auditor: atlas_specific.dumps.remove_cached_dump')

    for path in paths:
        os.remove(path)
        logger.debug(f"Removing dump: {path}")

    return True


