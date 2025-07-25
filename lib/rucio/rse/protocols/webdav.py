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

import logging
import os
import sys
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import urlparse
from xml.etree import ElementTree

import requests
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager

from rucio.common import exception
from rucio.rse.protocols import protocol


class TLSHTTPAdapter(HTTPAdapter):
    '''
    Class to force the SSL protocol to latest TLS
    '''
    def init_poolmanager(self, connections, maxsize, block=False):
        self.poolmanager = PoolManager(num_pools=connections,
                                       maxsize=maxsize,
                                       block=block,
                                       cert_reqs="CERT_REQUIRED",
                                       ca_cert_dir="/etc/grid-security/certificates")


class UploadInChunks:
    '''
    Class to upload by chunks.
    '''

    def __init__(self, filename, chunksize, progressbar=False):
        self.__totalsize = os.path.getsize(filename)
        self.__readsofar = 0
        self.__filename = filename
        self.__chunksize = chunksize
        self.__progressbar = progressbar

    def __iter__(self):
        try:
            with open(self.__filename, 'rb') as file_in:
                while True:
                    data = file_in.read(self.__chunksize)
                    if not data:
                        if self.__progressbar:
                            sys.stdout.write("\n")
                        break
                    self.__readsofar += len(data)
                    if self.__progressbar:
                        percent = self.__readsofar * 100 / self.__totalsize
                        sys.stdout.write("\r{percent:3.0f}%".format(percent=percent))
                    yield data
        except OSError as error:
            raise exception.SourceNotFound(error)

    def __len__(self):
        return self.__totalsize


class IterableToFileAdapter:
    '''
    Class IterableToFileAdapter
    '''
    def __init__(self, iterable):
        self.iterator = iter(iterable)
        self.length = len(iterable)

    def read(self, size=-1):   # TBD: add buffer for `len(data) > size` case
        nextvar = next(self.iterator, b'')
        return nextvar

    def __len__(self):
        return self.length


@dataclass(frozen=True)
class _PropfindFile:
    """Contains the properties of one file from a PROPFIND response."""

    href: str
    size: Optional[int]

    @classmethod
    def from_xml_node(cls, node: ElementTree.Element):
        """Extract file properties from a `<{DAV:}response>` node."""

        xml_href = node.find('./{DAV:}href')
        if xml_href is None or xml_href.text is None:
            raise ValueError('Response is missing mandatory field "href".')
        else:
            href = xml_href.text

        xml_size = node.find('./{DAV:}propstat/{DAV:}prop/{DAV:}getcontentlength')
        if xml_size is None or xml_size.text is None:
            size = None
        else:
            size = int(xml_size.text)

        return cls(href=href, size=size)  # type: ignore


@dataclass(frozen=True)
class _PropfindResponse:
    """Contains all the files from a PROPFIND response."""

    files: tuple[_PropfindFile]

    @classmethod
    def parse(cls, document: str):
        """Parses the XML document of a WebDAV PROPFIND response.

        The PROPFIND response is described in RFC 4918.
        This method expects the document root to be a node with tag `{DAV:}multistatus`.

        :param document: XML document to parse.
        :raises ValueError: if the XML document couldn't be parsed.
        :returns: The parsed response.
        """

        try:
            xml = ElementTree.fromstring(document)  # noqa: S314
        except ElementTree.ParseError as ex:
            raise ValueError("Couldn't parse XML document") from ex

        if xml.tag != '{DAV:}multistatus':
            raise ValueError('Root element is not "{DAV:}multistatus".')

        files = []
        for xml_response in xml.findall('./{DAV:}response'):
            files.append(_PropfindFile.from_xml_node(xml_response))

        return cls(files=tuple(files))  # type: ignore


class Default(protocol.RSEProtocol):

    """ Implementing access to RSEs using the webDAV protocol."""

    def connect(self, credentials: Optional[dict[str, Any]] = None) -> None:
        """ Establishes the actual connection to the referred RSE.

            :param credentials: Provides information to establish a connection
                to the referred storage system. For WebDAV connections these are
                ca_cert, cert, auth_type, timeout

            :raises RSEAccessDenied
        """
        credentials = credentials or {}
        using_presigned_urls = self.rse['sign_url'] is not None
        try:
            parse_url = urlparse(self.path2pfn(''))
            self.server = f'{parse_url.scheme}://{parse_url.netloc}'
        except KeyError:
            raise exception.RSEAccessDenied('No specified Server')

        try:
            self.ca_cert = credentials['ca_cert']
        except KeyError:
            self.ca_cert = None

        try:
            self.auth_type = credentials['auth_type']
        except KeyError:
            self.auth_type = 'cert'

        if using_presigned_urls:
            # Suppress all authentication, otherwise S3 servers will reject
            # requests.
            self.cert = None
            self.auth_token = None
        else:
            try:
                self.cert = credentials['cert']
            except KeyError:
                x509 = os.getenv('X509_USER_PROXY')
                if not x509:
                    # Trying to get the proxy from the default location
                    proxy_path = '/tmp/x509up_u%s' % os.geteuid()
                    if os.path.isfile(proxy_path):
                        self.cert = (proxy_path, proxy_path)
                    elif self.auth_token:
                        # If no proxy is found, we set the cert to None and use the auth_token
                        self.cert = None
                        pass
                    else:
                        raise exception.RSEAccessDenied('X509_USER_PROXY is not set')
                else:
                    self.cert = (x509, x509)

        try:
            self.timeout = credentials['timeout']
        except KeyError:
            self.timeout = 300
        self.session = requests.Session()
        self.session.mount('https://', TLSHTTPAdapter())
        if self.auth_token:
            self.session.headers.update({'Authorization': 'Bearer ' + self.auth_token})
        # "ping" to see if the server is available
        try:
            test_url = self.path2pfn('')
            res = self.session.request('HEAD', test_url, verify=False, timeout=self.timeout, cert=self.cert)
            # REVISIT: this test checks some URL that doesn't correspond to
            # any valid Rucio file.  Although this works for normal WebDAV
            # endpoints, it fails for endpoints using presigned URLs.  As a
            # work-around, accept 4xx status codes when using presigned URLs.
            if res.status_code != 200 and not (using_presigned_urls and res.status_code < 500):
                raise exception.ServiceUnavailable('Bad status code %s %s : %s' % (res.status_code, test_url, res.text))
        except requests.exceptions.ConnectionError as error:
            raise exception.ServiceUnavailable('Problem to connect %s : %s' % (test_url, error))
        except requests.exceptions.ReadTimeout as error:
            raise exception.ServiceUnavailable(error)

    def close(self):
        self.session.close()

    def path2pfn(self, path):
        """
            Returns a fully qualified PFN for the file referred by path.

            :param path: The path to the file.

            :returns: Fully qualified PFN.

        """
        if not path.startswith('https'):
            return '%s://%s:%s%s%s' % (self.attributes['scheme'], self.attributes['hostname'], str(self.attributes['port']), self.attributes['prefix'], path)
        else:
            return path

    def exists(self, pfn):
        """ Checks if the requested file is known by the referred RSE.

            :param pfn: Physical file name

            :returns: True if the file exists, False if it doesn't

            :raise  ServiceUnavailable, RSEAccessDenied
        """
        path = self.path2pfn(pfn)

        using_presigned_urls = self.rse['sign_url'] is not None

        try:
            # use GET instead of HEAD for presigned urls
            if not using_presigned_urls:
                result = self.session.request('HEAD', path, verify=False, timeout=self.timeout, cert=self.cert)
            else:
                result = self.session.request('GET', path, verify=False, timeout=self.timeout, cert=self.cert)
            if result.status_code == 200:
                return True
            elif result.status_code in [401, ]:
                raise exception.RSEAccessDenied()
            elif result.status_code in [404, ]:
                return False
            else:
                # catchall exception
                raise exception.RucioException(result.status_code, result.text)
        except requests.exceptions.ConnectionError as error:
            raise exception.ServiceUnavailable(error)

    def get(self, pfn, dest='.', transfer_timeout=None):
        """ Provides access to files stored inside connected the RSE.

            :param pfn: Physical file name of requested file
            :param dest: Name and path of the files when stored at the client
            :param transfer_timeout: Transfer timeout (in seconds)

            :raises DestinationNotAccessible, ServiceUnavailable, SourceNotFound, RSEAccessDenied
        """
        path = self.path2pfn(pfn)
        chunksize = 1024
        transfer_timeout = self.timeout if transfer_timeout is None else transfer_timeout

        try:
            result = self.session.get(path, verify=False, stream=True, timeout=transfer_timeout, cert=self.cert)
            if result and result.status_code in [200, ]:
                length = None
                if 'content-length' in result.headers:
                    length = int(result.headers['content-length'])
                with open(dest, 'wb') as file_out:
                    nchunk = 0
                    if not length:
                        print('Malformed HTTP response (missing content-length header).')
                    for chunk in result.iter_content(chunksize):
                        file_out.write(chunk)
                        if length:
                            nchunk += 1
            elif result.status_code in [404, ]:
                raise exception.SourceNotFound()
            elif result.status_code in [401, 403]:
                raise exception.RSEAccessDenied()
            else:
                # catchall exception
                raise exception.RucioException(result.status_code, result.text)
        except requests.exceptions.ConnectionError as error:
            raise exception.ServiceUnavailable(error)
        except requests.exceptions.ReadTimeout as error:
            raise exception.ServiceUnavailable(error)

    def put(self, source, target, source_dir=None, transfer_timeout=None, progressbar=False):
        """ Allows to store files inside the referred RSE.

            :param source: Physical file name
            :param target: Name of the file on the storage system e.g. with prefixed scope
            :param source_dir Path where the to be transferred files are stored in the local file system
            :param transfer_timeout Transfer timeout (in seconds)

            :raises DestinationNotAccessible, ServiceUnavailable, SourceNotFound, RSEAccessDenied
        """
        path = self.path2pfn(target)
        full_name = source_dir + '/' + source if source_dir else source
        directories = path.split('/')
        # Try the upload without testing the existence of the destination directory
        transfer_timeout = self.timeout if transfer_timeout is None else transfer_timeout

        try:
            if not os.path.exists(full_name):
                raise exception.SourceNotFound()
            it = UploadInChunks(full_name, 10000000, progressbar)
            result = self.session.put(path, data=IterableToFileAdapter(it), verify=False, allow_redirects=True, timeout=transfer_timeout, cert=self.cert)
            if result.status_code in [200, 201]:
                return
            if result.status_code in [409, ]:
                raise exception.FileReplicaAlreadyExists()
            else:
                # Create the directories before issuing the PUT
                for directory_level in reversed(list(range(1, 4))):
                    upper_directory = "/".join(directories[:-directory_level])
                    self.mkdir(upper_directory)
                try:
                    if not os.path.exists(full_name):
                        raise exception.SourceNotFound()
                    it = UploadInChunks(full_name, 10000000, progressbar)
                    result = self.session.put(path, data=IterableToFileAdapter(it), verify=False, allow_redirects=True, timeout=transfer_timeout, cert=self.cert)
                    if result.status_code in [200, 201]:
                        return
                    if result.status_code in [409, ]:
                        raise exception.FileReplicaAlreadyExists()
                    elif result.status_code in [401, ]:
                        raise exception.RSEAccessDenied()
                    else:
                        # catchall exception
                        raise exception.RucioException(result.status_code, result.text)
                except requests.exceptions.ConnectionError as error:
                    raise exception.ServiceUnavailable(error)
                except OSError as error:
                    raise exception.SourceNotFound(error)
        except requests.exceptions.ConnectionError as error:
            raise exception.ServiceUnavailable(error)
        except requests.exceptions.ReadTimeout as error:
            raise exception.ServiceUnavailable(error)
        except OSError as error:
            raise exception.SourceNotFound(error)

    def rename(self, pfn, new_pfn):
        """ Allows to rename a file stored inside the connected RSE.

            :param pfn:      Current physical file name
            :param new_pfn  New physical file name

            :raises DestinationNotAccessible, ServiceUnavailable, SourceNotFound, RSEAccessDenied
        """
        path = self.path2pfn(pfn)
        new_path = self.path2pfn(new_pfn)
        directories = new_path.split('/')

        headers = {'Destination': new_path}
        # Try the rename without testing the existence of the destination directory
        try:
            result = self.session.request('MOVE', path, verify=False, headers=headers, timeout=self.timeout, cert=self.cert)
            if result.status_code == 201:
                return
            elif result.status_code in [404, ]:
                raise exception.SourceNotFound()
            else:
                # Create the directories before issuing the MOVE
                for directory_level in reversed(list(range(1, 4))):
                    upper_directory = "/".join(directories[:-directory_level])
                    self.mkdir(upper_directory)
                try:
                    result = self.session.request('MOVE', path, verify=False, headers=headers, timeout=self.timeout, cert=self.cert)
                    if result.status_code == 201:
                        return
                    elif result.status_code in [404, ]:
                        raise exception.SourceNotFound()
                    elif result.status_code in [401, ]:
                        raise exception.RSEAccessDenied()
                    else:
                        # catchall exception
                        raise exception.RucioException(result.status_code, result.text)
                except requests.exceptions.ConnectionError as error:
                    raise exception.ServiceUnavailable(error)
        except requests.exceptions.ConnectionError as error:
            raise exception.ServiceUnavailable(error)
        except requests.exceptions.ReadTimeout as error:
            raise exception.ServiceUnavailable(error)

    def delete(self, pfn):
        """ Deletes a file from the connected RSE.

            :param pfn: Physical file name

            :raises ServiceUnavailable, SourceNotFound, RSEAccessDenied, ResourceTemporaryUnavailable
        """
        path = self.path2pfn(pfn)
        try:
            result = self.session.delete(path, verify=False, timeout=self.timeout, cert=self.cert)
            if result.status_code in [204, ]:
                return
            elif result.status_code in [404, ]:
                raise exception.SourceNotFound()
            elif result.status_code in [401, 403]:
                raise exception.RSEAccessDenied()
            elif result.status_code in [500, 503]:
                raise exception.ResourceTemporaryUnavailable()
            else:
                # catchall exception
                raise exception.RucioException(result.status_code, result.text)
        except requests.exceptions.ConnectionError as error:
            raise exception.ServiceUnavailable(error)
        except requests.exceptions.ReadTimeout as error:
            raise exception.ServiceUnavailable(error)

    def mkdir(self, directory):
        """ Internal method to create directories

            :param directory: Name of the directory that needs to be created

            :raises DestinationNotAccessible, ServiceUnavailable, SourceNotFound, RSEAccessDenied
        """
        path = self.path2pfn(directory)
        try:
            result = self.session.request('MKCOL', path, verify=False, timeout=self.timeout, cert=self.cert)
            if result.status_code in [201, 405]:  # Success or directory already exists
                return
            elif result.status_code in [404, ]:
                raise exception.SourceNotFound()
            elif result.status_code in [401, ]:
                raise exception.RSEAccessDenied()
            else:
                # catchall exception
                raise exception.RucioException(result.status_code, result.text)
        except requests.exceptions.ConnectionError as error:
            raise exception.ServiceUnavailable(error)
        except requests.exceptions.ReadTimeout as error:
            raise exception.ServiceUnavailable(error)

    def ls(self, filename):
        """ Internal method to list files/directories

            :param filename: Name of the directory that needs to be created

            :raises DestinationNotAccessible, ServiceUnavailable, SourceNotFound, RSEAccessDenied
        """
        path = self.path2pfn(filename)
        headers = {'Depth': '1'}
        self.exists(filename)
        try:
            result = self.session.request('PROPFIND', path, verify=False, headers=headers, timeout=self.timeout, cert=self.cert)
            if result.status_code in [404, ]:
                raise exception.SourceNotFound()
            elif result.status_code in [401, ]:
                raise exception.RSEAccessDenied()

            try:
                propfind = _PropfindResponse.parse(result.text)
            except ValueError:
                raise exception.ServiceUnavailable("Couldn't parse WebDAV response.")

            list_files = [self.server + file.href for file in propfind.files if file.href is not None]

            try:
                list_files.remove(filename + '/')
            except ValueError:
                pass
            try:
                list_files.remove(filename)
            except ValueError:
                pass

            return list_files
        except requests.exceptions.ConnectionError as error:
            raise exception.ServiceUnavailable(error)
        except requests.exceptions.ReadTimeout as error:
            raise exception.ServiceUnavailable(error)

    def stat(self, path):
        """
            Returns the stats of a file.

            :param path: path to file

            :raises ServiceUnavailable: if some generic error occurred in the library.
            :raises SourceNotFound: if the source file was not found on the referred storage.
            :raises RSEAccessDenied: in case of permission issue.

            :returns: a dict with filesize of the file provided in path as a key.
        """
        headers = {'Depth': '1'}
        dict_ = {}
        try:
            result = self.session.request('PROPFIND', path, verify=False, headers=headers, timeout=self.timeout, cert=self.cert)
            if result.status_code in [404, ]:
                raise exception.SourceNotFound()
            elif result.status_code in [401, ]:
                raise exception.RSEAccessDenied()
            if result.status_code in [400, ]:
                raise exception.InvalidRequest()
        except requests.exceptions.ConnectionError as error:
            raise exception.ServiceUnavailable(error)
        except requests.exceptions.ReadTimeout as error:
            raise exception.ServiceUnavailable(error)

        path_parts = self.parse_pfns(path)[path]
        local_path = os.path.join(path_parts['prefix'], path_parts['path'][1:], path_parts['name'])

        try:
            propfind = _PropfindResponse.parse(result.text)
        except ValueError:
            raise exception.ServiceUnavailable("Couldn't parse WebDAV response.")

        for file in propfind.files:
            if file.href != str(local_path):
                continue

            if file.size is None:
                continue

            dict_['filesize'] = file.size
            break
        else:
            raise exception.ServiceUnavailable("WebDAV response didn't include content length for requested path.")

        return dict_

    def get_space_usage(self):
        """
        Get RSE space usage information.

        :returns: a list with dict containing 'totalsize' and 'unusedsize'

        :raises ServiceUnavailable: if some generic error occurred in the library.
        """
        endpoint_basepath = self.path2pfn('')
        headers = {'Depth': '0'}

        try:
            root = ElementTree.fromstring(self.session.request('PROPFIND', endpoint_basepath, verify=False, headers=headers, cert=self.session.cert).text)  # noqa: S314
            usedsize = root[0][1][0].find('{DAV:}quota-used-bytes').text
            try:
                unusedsize = root[0][1][0].find('{DAV:}quota-available-bytes').text
            except Exception:
                print('No free space given, return -999')
                unusedsize = -999
            totalsize = int(usedsize) + int(unusedsize)
            return totalsize, unusedsize
        except Exception as error:
            raise exception.ServiceUnavailable(error)


class NoRename(Default):
    """ Implementing access to RSEs using the WebDAV protocol but without
    renaming files on upload/download. Necessary for some storage endpoints.
    """

    def __init__(self, protocol_attr, rse_settings, logger=logging.log):
        """ Initializes the object with information about the referred RSE.

            :param protocol_attr:  Properties of the requested protocol.
            :param rse_settings:   The RSE settings.
            :param logger:         Optional decorated logger that can be passed from the calling daemons or servers.
        """
        super(NoRename, self).__init__(protocol_attr, rse_settings, logger=logger)
        self.renaming = False
        self.attributes.pop('determinism_type', None)

    def rename(self, pfn, new_pfn):
        """ Allows to rename a file stored inside the connected RSE.

            :param pfn:      Current physical file name
            :param new_pfn  New physical file name

            :raises DestinationNotAccessible, ServiceUnavailable, SourceNotFound
        """
        raise NotImplementedError
