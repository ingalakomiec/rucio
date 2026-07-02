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
import glob
import logging
import os

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


def remove_cached_dumps(paths: []) -> bool:

    logger = logging.getLogger('auditor: output.remove_cached_dump')

    for path in paths:
        #remove = glob.glob(f"{cache_dir}/*{rse}*")
        remove = glob.glob(f"{path}*")
        for fil in remove:
            os.remove(fil)
    return True



