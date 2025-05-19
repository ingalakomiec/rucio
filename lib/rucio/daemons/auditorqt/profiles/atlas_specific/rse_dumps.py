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

from configparser import RawConfigParser
from datetime import datetime
from html.parser import HTMLParser

from rucio.common.dumper import ddmendpoint_url, gfal_download_to_file, http_download_to_file

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
    return ['/'.join((base_url, f)) for f in ctxt.listdir(str(base_url))]

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
            links.append('{0}/{1}'.format(base_url, link))
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


def generate_url(
    rse: str
#    config: RawConfigParser
) -> tuple[str, str]:

    print("generating url for rse")

    site = rse.split('_')[0]

#    if site not in config.sections():

#    base_url = f"{ddmendpoint_url(rse)}/test/dumps"
    base_url = f"{ddmendpoint_url(rse)}/test/80/25"
#    base_url = f"ddmendpoint_url(rse)/dumps"
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
        url_pattern: str,
        links: "Iterable[str]"
) -> tuple[str, datetime]:
    '''
    Returns a tuple with the newest url in the `links` list matching the
    pattern `url_pattern` and a datetime object representing the creation
    date of the url.

    The creation date is extracted from the url using datetime.strptime().
    '''
    logger = logging.getLogger('auditor.srmdumps')
    times = []

    pattern_components = url_pattern.split('/')

    date_pattern = '{0}/{1}'.format(base_url, pattern_components[0])
    if len(pattern_components) > 1:
        postfix = '/' + '/'.join(pattern_components[1:])
    else:
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
        logger.error(msg)
        raise RuntimeError(msg)

    return max(times, key=operator.itemgetter(1))

def protocol(url: str) -> str:
    '''
    Given the URL `url` returns a string with the protocol part.
    '''
    proto = url.split('://')[0]
    if proto not in protocol_funcs:
        raise RuntimeError('Protocol {0} not supported'.format(proto))

    return proto
