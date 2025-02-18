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

"""ATLAS-specific auditor profiles."""

import datetime
#import logging
from typing import Optional

#from rucio.core.did import get_metadata
from rucio.common.dumper import DUMPS_CACHE_DIR

#from .generic import _call_for_attention, generic_move

#if TYPE_CHECKING:
#    from rucio.common.types import LoggerFunction

#    from .types import DecommissioningProfile

def atlas_download_rse_dump(
        rse: str,
#        configuration: RawConfigParser,
        date: Optional[datetime.datetime] = None,
        destdir: str = DUMPS_CACHE_DIR
#) -> tuple[str, datetime.datetime]:
):
    '''
    Downloads the dump for the given ddmendpoint. If this endpoint does not
    follow the standardized method to publish the dumps it should have an
    entry in the `configuration` object describing how to download the dump.

    `rse` is the DDMEndpoint name.

    `configuration` is a RawConfigParser subclass.

    `date` is a datetime instance with the date of the desired dump or None
    to download the latest available dump.

    `destdir` is the directory where the dump will be saved (the final component
    in the path is created if it doesn't exist).

    Return value: a tuple with the filename and a datetime instance with
    the date of the dump.
    '''
    print(rse)

#    logger = logging.getLogger('auditor.srmdumps')

#    base_url, url_pattern = generate_url(rse, configuration)

#    if not os.path.isdir(destdir):
#        os.mkdir(destdir)

    # check for objectstores, which need to be handled differently
#    rse_id = get_rse_id(rse)
#    rse_attr = list_rse_attributes(rse_id)
#    if RseAttr.IS_OBJECT_STORE in rse_attr and rse_attr[RseAttr.IS_OBJECT_STORE] is not False:
#        tries = 1
#        if date is None:
            # on objectstores, can't list dump files, so try the last N dates
#            date = datetime.datetime.now()
#            tries = OBJECTSTORE_NUM_TRIES
#        path = ''
#        while tries > 0:
#            url = '{0}/{1}'.format(base_url, date.strftime(url_pattern))

#            filename = '{0}_{1}_{2}_{3}'.format(
#                'ddmendpoint',
#                rse,
#                date.strftime('%d-%m-%Y'),
#                hashlib.sha1(url.encode()).hexdigest()
#            )
#            filename = re.sub(r'\W', '-', filename)
#            path = os.path.join(destdir, filename)
#            if not os.path.exists(path):
#                logger.debug('Trying to download: "%s"', url)
#                if RseAttr.SIGN_URL in rse_attr:
#                    url = get_signed_url(rse_id, rse_attr[RseAttr.SIGN_URL], 'read', url)
#                try:
#                    with temp_file(destdir, final_name=filename) as (f, _):
#                        download(url, f)
#                    tries = 0
#                except (HTTPDownloadFailed, gfal2.GError):
#                    tries -= 1
#                    date = date - datetime.timedelta(1)
#    else:
#        if date is None:
#            logger.debug('Looking for site dumps in: "%s"', base_url)
#            links = get_links(base_url)
#            url, date = get_newest(base_url, url_pattern, links)
#        else:
#            url = '{0}/{1}'.format(base_url, date.strftime(url_pattern))

#        filename = '{0}_{1}_{2}_{3}'.format(
#            'ddmendpoint',
#            rse,
#            date.strftime('%d-%m-%Y'),
#            hashlib.sha1(url.encode()).hexdigest()
#        )
#        filename = re.sub(r'\W', '-', filename)
#        path = os.path.join(destdir, filename)

#        if not os.path.exists(path):
#            logger.debug('Trying to download: "%s"', url)
#            with temp_file(destdir, final_name=filename) as (f, _):
#                download(url, f)

#    return (path, date)
    return True




#def atlas_move(rse: dict[str, Any], config: dict[str, Any]) -> 'DecommissioningProfile':
    """Return a profile for moving rules that satisfy conditions to a specific destination.

    The "ATLAS move" profile lists out all rules that are locking replicas
    at the given RSE, and moves them to the specified destination if either
    one of the following is true:

    - The RSE expression of the rule is trivial (the RSE name itself).
    - There are no replicas locked by the rule that reside on another RSE.
    - The datatype of the DID is not "log".

    :param rse: RSE to decommission.
    :param config: Decommissioning configuration dictionary.
    :returns: A decommissioning profile dictionary.
    """
#    profile = generic_move(rse, config)
    # Insert before the trivial RSE expression handler
#    idx = next(pos for pos, handler in enumerate(profile.handlers)
#               if handler[0].__name__ == '_has_trivial_rse_expression')
#    profile.handlers.insert(idx, (_is_log_file, _call_for_attention))
#    return profile


#def _is_log_file(
#    rule: dict[str, Any],
#    rse: dict[str, Any],
#    *,
#    logger: "LoggerFunction" = logging.log
#) -> bool:
#    """Check if the datatype metadata is 'log'."""
#    return get_metadata(rule['scope'], rule['name'])['datatype'] == 'log'


#def generate_url(
#        rse: str,
#        config: RawConfigParser
#) -> tuple[str, str]:
    '''
    :param rse: Name of the endpoint.
    :param config: RawConfigParser instance which may have configuration
    related to the endpoint.
    :returns: Tuple with the URL where the links can be queried to find new
    dumps and the pattern used to parse the date of the dump of the files/directories
    listed..
    '''
#    site = rse.split('_')[0]
#    if site not in config.sections():
#        base_url = ddmendpoint_url(rse) + 'dumps'
#        url_pattern = 'dump_%Y%m%d'
#    else:
#        url_components = config.get(site, rse).split('/')
        # The pattern may not be the last component
#        pattern_index = next(idx for idx, comp in enumerate(url_components) if '%m' in comp)
#        base_url = '/'.join(url_components[:pattern_index])
#        url_pattern = '/'.join(url_components[pattern_index:])

#    return base_url, url_pattern


#def parse_configuration(conf_dirs: Optional[list[str]] = None) -> Parser:
    '''
    Parses the configuration for the endpoints contained in `conf_dir`.
    Returns a ConfParser.RawConfParser subclass instance.
    '''
#    conf_dirs = conf_dirs or __DUMPERCONFIGDIRS
#    logger = logging.getLogger('auditor.srmdumps')
#    if len(conf_dirs) == 0:
#        logger.error('No configuration directory given to load SRM dumps paths')
#        raise Exception('No configuration directory given to load SRM dumps paths')

#    configuration = Parser({
#        'disabled': False,
#    })

#    for conf_dir in conf_dirs:
#        configuration.read(glob.glob(conf_dir + '/*.cfg'))
#    return configuration
