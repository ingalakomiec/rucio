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

"""Consistency check for the auditor daemon"""

import logging
import os
import subprocess # noqa: S404 -- subprocess used for external commands
import tempfile

from datetime import datetime
from typing import Optional

from rucio.common.dumper import mkdir, smart_open, temp_file
#from rucio.common.dumper.consistency import Consistency


#    ALGORITHM 1
#    an algorithm with lists and a dictionary:
#    fast (7 min for DESY dumps),
#    not suitable for big (>4GB) dumps

def consistency_check_fast(
    rucio_dump_before_path: str,
    rse_dump_path: str,
    rucio_dump_after_path: str
) -> ([],[]):

    logger = logging.getLogger('auditorqt.consistencycheck.consistency_check_fast')
    logger.debug("Consistency check - fast")

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


#    ALGORITHM 2
#    an algorithm with open dump files and a dictionary:
#    fast, faster than ALGORITHM 1, 6.5 min for DESY dumps
#    not suitable for big (>4GB) dumps


def consistency_check_faster(
    rucio_dump_before_path: str,
    rse_dump_path: str,
    rucio_dump_after_path: str
) -> [[],[]]:

    logger = logging.getLogger('auditorqt.consistencycheck.consistency_check_faster')
    logger.debug("Consistency check - faster")

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

#    ALGORITHM 3
#    old algorithm
#    three dump files sorted opened
#    slow, 10.5 min for DESY dumps
#    suitable for big (>4GB) dumps


def consistency_check_slow_reliable(
    rucio_dump_before_path: str,
    rse_dump_path: str,
    rucio_dump_after_path: str,
    results_path: str,
    rse: str,
    date: datetime,
    cache_dir: str
) -> None:

    logger = logging.getLogger('auditorqt.consistencycheck.consistency_check_slow_reliable')
    logger.debug("Consistency check - slow, reliable")

    print("RELIABLE START")
# in consistency_check_slow... should be just dump before, dump after i dump rse

    """
    results = Consistency.dump(
        'consistency-manual',
        rse,
        rse_dump_path,
        rucio_dump_before_path,
        rucio_dump_after_path,
        date,
        cache_dir=cache_dir,
    )
    """

    rucio_dump_before_path_sorted = gnu_sort(
        parse_and_filter_file(rucio_dump_before_path, cache_dir=cache_dir, parser=parser),
        cache_dir=cache_dir,
        delimiter=',',
        fieldspec='1',
    )

    print("rucio dump before sorted")

    rucio_dump_after_path_sorted = gnu_sort(
        parse_and_filter_file(rucio_dump_after_path, cache_dir=cache_dir, parser=parser),
        cache_dir,
        delimiter=',',
        fieldspec='1',
    )

    print("rucio dump after sorted")

    standard_name_re = r'(ddmendpoint_{0}_\d{{2}}-\d{{2}}-\d{{4}}_[0-9a-f]{{40}})$'.format(rse)
    standard_name_match = re.search(standard_name_re, rse_dump_path)
    if standard_name_match is not None:
        # If the original filename was generated using the expected format,
        # just use the name as prefix for the parsed file.
        sd_prefix = standard_name_match.group(0)
    elif date is not None:
        # Otherwise try to use the date information and DDMEndpoint name to
        # have a meaningful filename.
        sd_prefix = 'ddmendpoint_{0}_{1}'.format(
            rse,
            date.strftime('%d-%m-%Y'),
        )
    else:
        # As last resort use only the DDMEndpoint name, but this is error
        # prone as old dumps may interfere with the checks.
        sd_prefix = 'ddmendpoint_{0}_unknown_date'.format(
            rse,
        )
        logger.warning(
            'Using basic and error prune naming for RSE dump as no date '
            'information was provided, %s dump will be named %s',
            rse,
            sd_prefix,
        )

    rse_dump_path_sorted = gnu_sort(
        parse_and_filter_file(
            rse_dump_path,
            cache_dir=cache_dir,
            parser=strip_storage_dump,
            prefix=sd_prefix,
        ),
        cache_dir=cache_dir,
        prefix=sd_prefix,
    )

    print("rse dump sorted")

    """
    with open(rucio_dump_before_path_sorted) as prevf:
        with open(rucio_dump_after_path_sorted ) as nextf:
            with open(rse_dump_path_sorted) as sdump:
                for path, where, status in compare3(prevf, sdump, nextf):
                    prevstatus, nextstatus = status

                    if where[0] and not where[1] and where[2]:
                        if prevstatus == 'A' and nextstatus == 'A':
                            yield cls('MISSING', path)

                        if not where[0] and where[1] and not where[2]:
                            yield cls('DARK', path)

    """
    """
    result_file_name = f"result.{rse}_{date:%Y%m%d}"

    with temp_file(results_path, final_name=result_file_name) as (output, _):
        for result in results:
            output.write('{0}\n'.format(result.csv()))
    """
    return True

def rucio_dump_before_pathprepare_rse_dump(
    dump_path: str
) -> []:

    logger = logging.getLogger('auditorqt.consistencycheck.prepare_rse_dump')
    logger.debug("Preparing RSE dump")

    file_rse_dump = smart_open(dump_path)
    rse_dump = file_rse_dump.readlines()
    file_rse_dump.close()

    return rse_dump

def prepare_rucio_dump(
    dump_path: str
) -> [[],[]]:

    logger = logging.getLogger('auditorqt.consistencycheck.prepare_rucio_dump')
    logger.debug("Preparing Rucio dump")

    rucio_dump = [[],[]]

    with smart_open(dump_path) as file_rucio_dump:

        for line in file_rucio_dump:
            rucio_dump[0].append(line.split()[7]+'\n')
            rucio_dump[1].append(line.split()[10])

        file_rucio_dump.close()

    return rucio_dump

def parser(line: str) -> str:
    '''
    Simple parser for Rucio replica dumps.

    :param line: String with one line of a dump.
    :returns: A tuple with the path and status of the replica.
    '''
    fields = line.split('\t')
    path = fields[6].strip().lstrip('/')
    status = fields[8].strip()

    return ','.join((path, status))

def parse_and_filter_file(
        filepath: str,
        cache_dir: str,
        parser: 'Callable' = lambda s: s,
        filter_: 'Callable' = lambda s: s,
        prefix: Optional[str] = None,
        postfix: str = 'parsed'
) -> str:
    '''
    Opens `filepath` as a read-only file, and for each line of the file
    for which the `filter_` function returns True, it writes a version
    parsed with the `parser` function.

    The name of the output file is generated appending '_' + `postfix` to
    the filename in `filepath`. If `prefix` is given it is used instead
    of `filepath`.

    The output file (and temporary files while processing are stored in
    `cache_dir`.

    Default values for the arguments:
        - `parser`: returns the same string.
        - `filter_`: returns True for any argument.
        - `prefix`: None (the name of the input file is used as prefix).
        - `postfix`: 'parsed'.
        - `cache_dir`: DUMPS_CACHE_DIR.

    The output file is created with a random name and renamed atomically
    when it is complete.

    '\n' is appended to each line, therefore if the input is 'a\nb\n' and `parser`
    is not especified the output will be 'a\n\nb\n\n'
    '''

    prefix = os.path.basename(filepath) if prefix is None else prefix
    output_name = '_'.join((prefix, postfix))
    output_path = os.path.join(cache_dir, output_name)

    if os.path.exists(output_path):
        return output_path

    with temp_file(cache_dir, final_name=output_name) as (output, _):
        input_ = smart_open(filepath)
        if input_ is not None:
            for line in input_:
                if filter_(line):
                    output.write(parser(line) + '\n')

            input_.close()

    return output_path

def gnu_sort(
        file_path: str,
        cache_dir: str,
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
        fieldspec: Optional[str] = None
) -> str:
    '''
    Sort the file with path `file_path` using the GNU sort command, the
    original file is unchanged, the output file is saved with path
    <cache_dir>/<prefix>_sorted.

    :param prefix: If given the output file will be named <prefix>_sorted.
    Otherwise the prefix is the name of the input file.
    :param delimiter: Delimiter character if the data is formatted in
    columns (argument of -t in the sort command).
    :param fieldspec: String with the specification of column or columns
    to be used to sort (argument -k in the sort command).
    :param cachedir: Working dir where the output file will be placed.

    Note: Using GNU sort to sort large files is convenient as it has low
    memory and it is relatively fast if used with the environment variable
    LC_ALL set to C as in this function.
    '''
    if (delimiter is not None) ^ (fieldspec is not None):
        raise ValueError("Either both delimiter and fieldspec is set, or neither are.")
    if delimiter is None:
        cmd_line = 'LC_ALL=C sort {0} > {1}'
    else:
        cmd_line = 'LC_ALL=C sort -t {0} -k {1} {{0}} > {{1}}'.format(delimiter, fieldspec)

    prefix = os.path.basename(file_path) if prefix is None else prefix

    sorted_name = '_'.join((prefix, 'sorted'))
    sorted_path = os.path.join(cache_dir, sorted_name)

    if os.path.exists(sorted_path):
        return sorted_path

    tfile = tempfile.NamedTemporaryFile(dir=cache_dir, delete=False)

    subprocess.check_call(
        cmd_line.format(file_path, tfile.name),
        shell=True,
    )

    os.link(tfile.name, sorted_path)
    os.unlink(tfile.name)

    return sorted_path

def strip_storage_dump(line: str) -> str:
    '''
    Parser to have consistent paths in storage dumps.

    :param line: String with one line of a dump.
    :returns: Path formatted as in the Rucio Replica Dumps.
    '''
    relative = path_parsing.remove_prefix(
        prefix_components,
        path_parsing.components(line),
    )
    if relative[0] == 'rucio':
        relative = relative[1:]
    return '/'.join(relative)


