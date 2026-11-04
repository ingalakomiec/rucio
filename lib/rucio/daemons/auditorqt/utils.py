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

# utils for auditorqt

import logging

from magic import Magic

CHUNK_SIZE = 4194304  # 4 MiB


def decode_chunk(chunk: bytes) -> str:
    """
    Detect the encoding of a byte chunk and decode it.
    """

    m = Magic(mime_encoding=True)
    encoding = m.from_buffer(chunk)
    decoded_chunk = chunk.decode(encoding)

    return decoded_chunk


def copy_and_decode_local_file(
    source_path: str,
    destination_path: str,
) -> None:

    logger = logging.getLogger('auditorqt.copy_and_decode_local_file')

    with open(source_path, 'rb') as source_file:
        with open(destination_path, 'w', encoding='utf-8') as destination_file:
            chunk = source_file.read(CHUNK_SIZE)

            while chunk:
                try:
                    decoded_chunk = decode_chunk(chunk)
                except UnicodeDecodeError as e:
                    logger.error(
                        f"UnicodeDecodeError occurred: {str(e)}, for source dump: {source_path}"
                    )
                    raise

                destination_file.write(decoded_chunk)
                chunk = source_file.read(CHUNK_SIZE)
