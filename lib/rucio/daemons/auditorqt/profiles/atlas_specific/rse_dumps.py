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

"""fetching ATLAS-RSE dumps"""

import gfal2
import hashlib
import logging
import operator
import os
import re

from configparser import RawConfigParser
from datetime import datetime, timedelta
from html.parser import HTMLParser
from typing import IO, Optional

from rucio.common.constants import RseAttr
from rucio.common.dumper import HTTPDownloadFailed, ddmendpoint_url, gfal_download_to_file, http_download_to_file, temp_file
from rucio.core.credential import get_signed_url
from rucio.core.rse import get_rse_id, list_rse_attributes

class _LinkCollector(HTMLParser):
    def __init__(self):
        super(_LinkCollector, self).__init__()
        self.links = []

    def handle_starttag(
            self, tag: str,
            attrs: "Iterable[tuple[str, str]]"
    ) -> None:
        if tag == 'a':
            self.links.append(
                next(value for key, value in attrs if key == 'href')
            )


def gfal_links(base_url: str) -> list[str]:
    '''
    Returns a list of the urls contained in `base_url`.
    '''
    ctxt = gfal2.creat_context()  # pylint: disable=no-member

    files_tmp = ['dump_20250610', 'dump_20250614', 'dump_20250521']

#    list = [f"{base_url}/{file}" for file in files_tmp]
    list = [f"{base_url}/{file}" for file in ctxt.listdir(str(base_url))]

    return list

def http_links(base_url: str) -> list[str]:
    '''
    Returns a list of the urls contained in `base_url`.
    '''
    html = requests.get(base_url).text
    link_collector = _LinkCollector()

    link_collector.feed(html)
    links = []
    for link in link_collector.links:
        if not link.startswith('http://') and not link.startswith('https://'):
            links.append({base_url}/{link})
        else:
            links.append(link)
    return links


protocol_funcs = {
    'davs': {
        'links': gfal_links,
        'download': gfal_download_to_file,
    },
    'root': {
        'links': gfal_links,
        'download': gfal_download_to_file,
    },
    'http': {
        'links': http_links,
        'download': http_download_to_file,
    },
    'https': {
        'links': http_links,
        'download': http_download_to_file,
    },
}

def download(url: str, filename: IO) -> None:
    """
    Given the URL 'url' downloads its contents on 'filename'
    """

    return protocol_funcs[protocol(url)]['download'](url, filename)

def fetch_object_store(
    rse: str,
    base_url: str,
    cache_dir: str,
    date: Optional[datetime] = None,
):

    # on objectstores can't list dump files, so try the last N dates

    logger = logging.getLogger('auditor.fetch_object_store')

    tries = 30

    if date is None:
        date = datetime.now()
        tries = 31

    while tries > 0:
        url = f"{base_url}/dump_{date:%Y%m%d}"
        # hash added to the file name to get a distinct name
        hash = hashlib.sha1(url.encode()).hexdigest()

        filename = f"ddmendpoint_{rse}_{date:%d-%m-%Y}_{hash}"
        filename = re.sub(r'\W', '-', filename)

        path = f"{cache_dir}/{filename}"

        rse_id = get_rse_id(rse)
        rse_attr = list_rse_attributes(rse_id)

        if not os.path.exists(path):
            logger.debug('Trying to download: "%s"', url)

            if RseAttr.SIGN_URL in rse_attr:
                url = get_signed_url(rse_id, rse_attr[RseAttr.SIGN_URL], 'read', url)

            try:
                with temp_file(cache_dir, final_name=filename) as (f, _):
                    download(url,f)
                tries = 0
            except (HTTPDownloadFailed, gfal2.GError):
                tries -= 1
                date = date - timedelta(1)

    return path, date


def fetch_no_object_store(
    rse: str,
    base_url: str,
    cache_dir: str,
    date: Optional[datetime] = None,
):

    logger = logging.getLogger('auditor.fetch_no_object_store')

    date = None
    if date is None:
        logger.debug('Looking for site dumps in: "%s"', base_url)
        links = get_links(base_url)
        url, date =  get_newest(base_url, links)
        print("url from get_newest: ", url)
        print("date from get_newest: ", date)
    else:
        url = f"{base_url}/dump_{date:%Y%m%d}"

    # hash added to get a distinct file name
    hash = hashlib.sha1(url.encode()).hexdigest()
    filename = f"ddmendpoint_{rse}_{date:%d-%m-%Y}_{hash}"
    filename = re.sub(r'\W', '-', filename)
    path = f"{cache_dir}/{filename}"

    if not os.path.exists(path):
#        logger.debug('Taking RSE Dump %s for %s from cache', path, rse)
#        return path
        logging.debug('Trying to download: %s for %s', url, rse)
        with temp_file(cache_dir, final_name=filename) as (f, _):
            download(url, f)

    return path, date


def generate_url(
    rse: str
#    config: RawConfigParser
) -> tuple[str, str]:

    site = rse.split('_')[0]
#    uncomment when the config part is added
#    if site not in config.sections():

    # base_url for real dumps
    base_url = f"{ddmendpoint_url(rse)}/dumps"

    # tmp base_url for the test RSE - XRD1
#    base_url = f"{ddmendpoint_url(rse)}/test/80/25"
    """
    else:

    url_components = config.get(site, rse).split('/')
    pattern_index = next(idx for idx, comp in enumerate(url_components) if '%m' in comp)
    base_url = '/'.join(url_components[:pattern_index])
    url_pattern = '/'.join(url_components[pattern_index:])
    """

    return base_url

def get_links(base_url: str) -> list[str]:

    return protocol_funcs[protocol(base_url)]['links'](base_url)

def get_newest(
        base_url: str,
        links: "Iterable[str]"
) -> tuple[str, datetime]:
    '''
    Returns a tuple with the newest url in the `links` list matching the
    pattern `url_pattern` and a datetime object representing the creation
    date of the url.

    The creation date is extracted from the url using datetime.strptime().
    '''
    logger = logging.getLogger('auditor.rse_dumps')
    times = []

#    url_pattern = 'dump_%Y%m%d'
#    pattern_components = url_pattern.split('/')
#    date_pattern = '{0}/{1}'.format(base_url, pattern_components[0])

    date_pattern = f"{base_url}/dump_%Y%m%d"

    """
    if len(pattern_components) > 1:
        postfix = '/' + '/'.join(pattern_components[1:])
    else:
        postfix = ''
    """
    postfix = ''


    for link in links:
        try:
            time = datetime.strptime(link, date_pattern)
        except ValueError:
            pass
        else:
            times.append((str(link) + postfix, time))

    if not times:
        msg = 'No links found matching the pattern {0} in {1}'.format(date_pattern, links)

        msg = f"No links found matching the pattern {date_pattern} in {links}"
        logger.error(msg)
        raise RuntimeError(msg)

    return max(times, key=operator.itemgetter(1))

#    return ('aaa', datetime.now())

def protocol(url: str) -> str:
    '''
    Given the URL `url` returns a string with the protocol part.
    '''
    proto = url.split('://')[0]
    if proto not in protocol_funcs:
        raise RuntimeError(f"Protocol {proto} not supported")

    return proto
