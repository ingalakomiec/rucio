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

"""Generic auditor profiles."""

import logging
import hashlib
#import numpy
import os
import re
import shutil

from datetime import datetime, timedelta
from typing import Optional

#for benchmarking
#from memory_profiler import profile

from rucio.common.dumper import smart_open
from rucio.daemons.auditorqt.profiles.atlas_specific.dumps import remove_cached_dumps
#from rucio.daemons.auditorqt.profiles.atlas_specific.output import process_output

#@profile
def generic_auditor(
        rse: str,
        keep_dumps: bool,
        delta: int,
        date: datetime,
        cache_dir: str,
        results_dir: str,
        no_declaration: bool
) -> Optional[str]:

    """
    `rse` is the RSE name

    'keep_dumps' keep RSE and Rucio dumps on cache or not

    'delta' How many days older/newer than the RSE dump must the Rucio replica dumps be

    `date` is a datetime instance with the date of the desired dump or None
    to download the latest available dump

    'cache_dir' dierectory where the dumps are cached

    `results_dir` is the directory where the results of the consistency check will be saved

    Return value: path to results
    """

    logger = logging.getLogger('generic_auditor')

    if date is None:
        date = datetime.now()

    delta = timedelta(delta)

#   paths to rse and rucio dumps
    rse_dump_path = '/opt/rucio/lib/rucio/daemons/auditorqt/tmp/real_dumps/dump_20250127.bz2'
    rucio_dump_before_path = '/opt/rucio/lib/rucio/daemons/auditorqt/tmp/real_dumps/rucio_dump_before/rucio_before.DESY-ZN_DATADISK_2025-01-24.bz2'
    rucio_dump_after_path = '/opt/rucio/lib/rucio/daemons/auditorqt/tmp/real_dumps/rucio_dump_after/rucio_after.DESY-ZN_DATADISK_2025-01-30.bz2'

# big dumps
#    rse_dump_path = '/opt/rucio/lib/rucio/daemons/auditorqt/tmp/real_dumps/big_dumps/BNL-OSG2_DATADISK.dump_20250805'
#    rucio_dump_before_path = '/opt/rucio/lib/rucio/daemons/auditorqt/tmp/real_dumps/big_dumps/BNL-OSG2_DATADISK_2025-08-02.bz2'
#    rucio_dump_after_path = '/opt/rucio/lib/rucio/daemons/auditorqt/tmp/real_dumps/big_dumps/BNL-OSG2_DATADISK_2025-08-08.bz2'

    rse_dump_path_cache, date_rse = fetch_rse_dump(rse_dump_path, rse, cache_dir, date)
    rucio_dump_before_path_cache = fetch_rucio_dump(rucio_dump_before_path, rse, date_rse - delta, cache_dir)
    rucio_dump_after_path_cache = fetch_rucio_dump(rucio_dump_after_path, rse, date_rse + delta, cache_dir)

    cached_dumps = [rucio_dump_before_path_cache, rse_dump_path_cache, rucio_dump_after_path_cache]

    result_file_name = f"result.{rse}_{date:%Y%m%d}"
    results_path = f"{results_dir}/{result_file_name}"

    if os.path.exists(f"{results_path}") or os.path.exists(f"{results_path}.bz2"):
        logger.warning(f"Consistency check for {rse}, dump dated {date_rse:%d-%m-%Y}, already done. Skipping consistency check.")
        if not keep_dumps:
            remove_cached_dumps(cached_dumps)
        return results_path

    missing_files, dark_files = consistency_check(rucio_dump_before_path_cache, rse_dump_path_cache, rucio_dump_after_path_cache, results_path)

#    consistency_check(rucio_dump_before_path_cache, rse_dump_path_cache, rucio_dump_after_path_cache, results_path)

    file_results = open(results_path, 'w')

    for k in range(len(dark_files)):
        file_results.write('DARK'+(dark_files[k]).replace("/",",",1))

#    for h in dark_files:
#        file_results.write('DARK,' + h.hex() + '\n')

#missing
    for k in range(len(missing_files)):
        file_results.write('MISSING'+(missing_files[k]).replace("/",",",1))

#    for h in missing_files:
#        file_results.write('MISSING,' + h.hex() + '\n')

    file_results.close()

    """
    if no_declaration:
        logger.warning(f"No action on output performed")
    else:
        process_output(rse, results_path)
    """

    if not keep_dumps:
        # taken from the atlas profile
        remove_cached_dumps(cached_dumps)

    return results_path

def fetch_rse_dump(
    source_path: str,
    rse: str,
    cache_dir: str,
    date: Optional[datetime] = None,
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

def fetch_rucio_dump(
    source_path: str,
    rse: str,
    date: "datetime",
    cache_dir: str
) -> str:

    logger = logging.getLogger('auditor.fetch_rucio_dump')

    # hash added to get a distinct file name
    hash = hashlib.sha1(source_path.encode()).hexdigest()
    filename = f"{rse}_{date:%d-%m-%Y}_{hash}"
    filename = re.sub(r'\W', '-', filename)
    final_path = f"{cache_dir}/{filename}"

    shutil.copyfile(source_path, final_path)

    logger.debug(f"Rucio dump before taken from: {source_path} and cached in: {final_path}")

    return final_path

#@profile
def prepare_rse_dump(
    dump_path: str
) -> []:

    logger = logging.getLogger('auditor.prepare_rse_dump')
    logger.debug("Preparing RSE dump")

    file_rse_dump = smart_open(dump_path)
    rse_dump = file_rse_dump.readlines()
    file_rse_dump.close()

    return rse_dump

#@profile
def prepare_rucio_dump(
    dump_path: str
) -> [[],[]]:

    logger = logging.getLogger('auditor.prepare_rucio_dump')
    logger.debug("Preparing Rucio dump")

    rucio_dump = [[],[]]

    with smart_open(dump_path) as file_rucio_dump:

        for line in file_rucio_dump:
            rucio_dump[0].append(line.split()[7]+'\n')
            rucio_dump[1].append(line.split()[10])

        file_rucio_dump.close()


    return rucio_dump

class SlottedRecord:
    __slots__ = ['value']
    def __init__(self, value):
        self.value = value

"""
class Entry:
    __slots__ = ['key', 'value']

    def __init__(self, key: str, value: int):
        self.key = key
        self.value = value

class MyHashTable:
    def __init__(self, initial_capacity=2**22):  # ~1 million slots - 2**20
        self.capacity = initial_capacity
        self.table = [None] * self.capacity
        self.size = 0

    def _hash(self, key):
        # Use Python's built-in hash function and confine to table size
        return hash(key) % self.capacity

    def set(self, key, value):
        idx = self._hash(key)
        start_idx = idx
        while True:
            entry = self.table[idx]
            if entry is None or entry.key == key:
                self.table[idx] = Entry(key, value)
                return
            idx = (idx + 1) % self.capacity
            if idx == start_idx:
                raise RuntimeError("Hash table is full")

    def get(self, key):
        idx = self._hash(key)
        start_idx = idx
        while True:
            entry = self.table[idx]
            if entry is None:
                raise KeyError(key)
            if entry.key == key:
                return entry.value
            idx = (idx + 1) % self.capacity
            if idx == start_idx:
                raise KeyError(key)

    def update(self, key, delta):
        idx = self._hash(key)
        start_idx = idx
        while True:
            entry = self.table[idx]
            if entry is None:
                raise KeyError(key)
            if entry.key == key:
                entry.value += delta
                return
            idx = (idx + 1) % self.capacity
            if idx == start_idx:
                raise KeyError(key)

    def delete(self, key):
        idx = self._hash(key)
        start_idx = idx
        while True:
            entry = self.table[idx]
            if entry is None:
                return  # Key not present
            if entry.key == key:
                self.table[idx] = None
                return
            idx = (idx + 1) % self.capacity
            if idx == start_idx:
                return

    def contains(self, key):
        idx = self._hash(key)
        start_idx = idx
        while True:
            entry = self.table[idx]
            if entry is None:
                return False
            if entry.key == key:
                return True
            idx = (idx + 1) % self.capacity
            if idx == start_idx:
                return False

    def items(self):
        for entry in self.table:
            if entry is not None:
                yield entry.key, entry.value
"""

#def hash_key(s):
#    return hashlib.md5(s.encode()).digest()

#def hash_key(s: str) -> bytes:
#    return hashlib.blake2b(s.encode('utf-8'), digest_size=8).digest()

"""
entry_dtype = numpy.dtype([
    ('hash', numpy.uint64),
    ('value', numpy.uint8)
], align=False)

MAX_ENTRIES = 250_000_000
hash_table = numpy.zeros(MAX_ENTRIES, dtype = entry_dtype)
entry_count = 0
"""
def hash_key(key_str: str) -> int:
    return int.from_bytes(
        hashlib.blake2b(key_str.encode('utf-8'), digest_size=8).digest(), 'big'
)

hash_index_map = {}

def insert_or_update(key_str, value_delta):
    global entry_count

    h = hash_key(key_str)

    if h in hash_index_map:
        idx = hash_index_map[h]
        hash_table[idx]['value'] += value_delta
        if hash_table[idx]['value'] == 0:
            # Remove if value becomes 0
            hash_table[idx]['value'] = 0
            del hash_index_map[h]
    else:
        if entry_count >= MAX_ENTRIES:
            raise MemoryError("Exceeded preallocated table size")

        idx = entry_count
        hash_table[idx] = (h, value_delta)
        hash_index_map[h] = idx
        entry_count += 1


#@profile
def consistency_check(
    rucio_dump_before_path: str,
    rse_dump_path: str,
    rucio_dump_after_path: str,
    results_path: str
#) -> ([],[]):
) -> None:
    logger = logging.getLogger('auditor.consistency_check')
    logger.debug("Consistency check")

#    entry_dtype = numpy.dtype([
#        ('hash', numpy.uint64),
#        ('value', numpy.uint8)
#    ])

#    MAX_ENTRIES = 25_000_000

    """
    hash_table = numpy.zeros(MAX_ENTRIES, dtype = entry_dtype)

    print("Pre-entry size:", entry_dtype.itemsize)

    with smart_open(rucio_dump_before_path) as file_rucio_dump_before:

        for line in file_rucio_dump_before:
            parts = line.strip().split()
            key = parts[7]+'\n'
            value = 16
            if parts[10]=='A':
                value += 2
            insert_or_update(key, value)

    print("done")

    """
    """
    with smart_open(rse_dump_path) as file_rse_dump:

        for line in file_rse_dump:
            key = line
            hkey = hash_key(key)
            if hkey in hash_index_map:
                idx = hash_index_map[hkey]
                del hash_index_map[hkey]
            else:
                insert_or_update(key, 8)

    """
    """
    with smart_open(rucio_dump_after_path) as file_rucio_dump_after:

        for line in file_rucio_dump_after:
            parts = line.strip().split()
            key = parts[7]+'\n'
            if key in out:
                out[key]+=4
                if parts[10]=='A':
                    out[key]+=1

    missing_files = [k for k in out if out[k]==23]
    dark_files = [k for k in out if out[k]==8]
    """
    """
    return True
    """
    """

    in_rucio_dump_before = set()
    #in_rucio_dump_after = set()
    #in_all_rucio_dumps = set()
    #in_rse_dump = set()

    print("rucio dump before")
    with smart_open(rucio_dump_before_path) as file_rucio_dump_before:
        for line in file_rucio_dump_before:
            parts = line.strip().split()
            key = parts[7]
            hkey = hash_key(key)
            #in_all_rucio_dumps.add(hkey)
            if parts[10] == 'A':
                in_rucio_dump_before.add(hkey)

    print("rucio dump after")

    with smart_open(rucio_dump_after_path) as file_rucio_dump_after:
        for line in file_rucio_dump_after:
            parts = line.strip().split()
            key = parts[7]
            hkey = hash_key(key)
            in_all_rucio_dumps.add(hkey)
            if parts[10] == 'A':
                in_rucio_dump_after.add(hkey)

    print("rse dump")
    with smart_open(rse_dump_path) as file_rse_dump:
        for line in file_rse_dump:
            key = line.strip()
            hkey = hash_key(key)
            in_rse_dump.add(hkey)

    with open(results_path, 'w') as file_out:
        for h in in_rse_dump:
            if h not in in_all_rucio_dumps:
                file_out.write("DARK," + h.hex() + "\n")

        for h in in_rucio_dump_before:
            if h in in_rucio_dump_after and h not in in_rse_dump:
                file_out.write("MISSING," + h.hex() + "\n")

    return True

    """
    """

    #out = dict()
    out = {}
   # out = MyHashTable()

    print("rucio dump before")
    with smart_open(rucio_dump_before_path) as file_rucio_dump_before:

        for line in file_rucio_dump_before:
            parts = line.strip().split()
            #key = parts[7]+'\n'
            key = parts[7]
            hkey = hash_key(key)
            #out[key] = 16
            #out[key] = SlottedRecord(16)
            out[hkey] = SlottedRecord(16)
            #out.set(key, 16)
            if parts[10]=='A':
                #out[key]+=2
                #out[key].value += 2
                out[hkey].value += 2
                #out.update(key, 2)
            else:
                #del out[key]
                del out[hkey]
                #out.delete(key)

    print("rse dump")
    with smart_open(rse_dump_path) as file_rse_dump:

        for line in file_rse_dump:
            hkey = hash_key(line.strip())
            #if line in out:
            if hkey in out:
            #if out.contains(line):
                #del out[line]
                del out[hkey]
                #out.delete(line)
            else:
                #out[line]=8
                #out[line] = SlottedRecord(8)
                out[hkey] = SlottedRecord(8)
                #out.set(line, 8)

    print("rucio dump after")
    with smart_open(rucio_dump_after_path) as file_rucio_dump_after:

        for line in file_rucio_dump_after:
            parts = line.strip().split()
            #key = parts[7]+'\n'
            key = parts[7]
            hkey = hash_key(key)
            #if key in out:
            if hkey in out:
            #if out.contains(key):
                #out[key]+=4
                #out[key].value += 4
                out[hkey].value += 4
                #out.update(key, 4)
                if parts[10]=='A':
                    #out[key]+=1
                    #out[key].value += 1
                    out[hkey].value += 1
                    #out.update(key,1)
                else:
                    #del out[key]
                    del out[hkey]
                    #out.delete(key)

    #missing_files = [k for k in out if out[k]==23]
    #dark_files = [k for k in out if out[k]==8]


    missing_files = [k for k, v in out.items() if v.value == 23]
    dark_files = [k for k, v in out.items() if v.value == 8]

    results = (missing_files, dark_files)

    return results
    """


    """
    with open('auditor-cache/output_tmp.txt', 'w') as out_file:

        with smart_open(rucio_dump_before_path) as file_rucio_dump_before:

            for line in file_rucio_dump_before:
                parts = line.strip().split()
                key = parts[7]+'\n'
                value = 16
                if parts[10]=='A':
                    value+=2
                out_file.write(f"{key}\t{value}\n")
    """

    out = dict()

    with smart_open(rucio_dump_before_path) as file_rucio_dump_before:

        for line in file_rucio_dump_before:
            parts = line.strip().split()
            key = parts[7]+'\n'
            out[key] = 16
            if parts[10]=='A':
                out[key]+=2

    with smart_open(rse_dump_path) as file_rse_dump:

        for line in file_rse_dump:
            if line in out:
                out[line]+=8
            else:
                out[line]=8

    with smart_open(rucio_dump_after_path) as file_rucio_dump_after:

        for line in file_rucio_dump_after:
            parts = line.strip().split()
            key = parts[7]+'\n'
            if key in out:
                out[key]+=4
                if parts[10]=='A':
                    out[key]+=1
            else:
                out[key]=4

    missing_files = [k for k in out if out[k]==23]
    dark_files = [k for k in out if out[k]==8]

    results = (missing_files, dark_files)

    return results

    """

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

    missing_files = [k for k in out if out[k]==23]
    dark_files = [k for k in out if out[k]==8]

    results = (missing_files, dark_files)

    return results
    """

    """
    rucio_dump_before = prepare_rucio_dump(rucio_dump_before_path)

    file_states = {}

    for file, status in zip(rucio_dump_before[0], rucio_dump_before[1]):
        file_states[file] = 16 + (2 if status == 'A' else 0)

    del rucio_dump_before

    rse_files = prepare_rse_dump(rse_dump_path)

    for file in rse_files:
        file_states[file] = file_states.get(file, 0) + 8

    del rse_files
    rucio_dump_after = prepare_rucio_dump(rucio_dump_after_path)

    for file, status in zip(rucio_dump_after[0], rucio_dump_after[1]):
        file_states[file] = file_states.get(file, 0) + 4 + (1 if status == 'A' else 0)

    del rucio_dump_after

    missing_files = [file for file, state in file_states.items() if state == 23]
    dark_files = [file for file, state in file_states.items() if state == 8]

    return missing_files, dark_files
    """
