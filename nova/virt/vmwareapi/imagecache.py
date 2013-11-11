# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2013 OpenStack Foundation
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""
Image cache class
"""

from oslo.config import cfg

from nova.openstack.common.gettextutils import _
from nova.openstack.common import log as logging
from nova.openstack.common import timeutils
from nova.virt import imagecache
from nova.virt.vmwareapi import ds_util
from nova.virt.vmwareapi import vim_util

LOG = logging.getLogger(__name__)

CONF = cfg.CONF
CONF.import_opt('remove_unused_original_minimum_age_seconds',
                'nova.virt.imagecache')

TIMESTAMP_PREFIX = 'ts-'
TIMESTAMP_FORMAT = '%Y-%m-%d-%H-%M-%S'


class ImageCacheManager(imagecache.ImageCacheManager):
    def __init__(self, session, base_folder):
        super(ImageCacheManager, self).__init__()
        self._session = session
        self._base_folder = base_folder
        self._ds_browser = {}

    def timestamp_cleanup(self, instance, dc_ref, ds_browser,
                          ds_ref, ds_name, ds_path):
        ts = self._get_timestamp(ds_browser, ds_path)
        if ts:
            ts_path = '%s/%s' % (ds_path, ts)
            LOG.debug(_("Timestamp path %s exists. Deleting!"), ts_path)
            ds_util.file_delete(self._session, instance, ts_path, dc_ref)

    def _get_timestamp(self, ds_browser, ds_path):
        # A list of files in ds_path will be returned - we need to get
        # the timestamp file if it exists
        files = ds_util.get_sub_folders(self._session, ds_browser, ds_path)
        if files:
            for file in files:
                if file.startswith(TIMESTAMP_PREFIX):
                    return file

    def _get_timestamp_filename(self):
        return '%s%s' % (TIMESTAMP_PREFIX,
                         timeutils.strtime(fmt=TIMESTAMP_FORMAT))

    def _get_datetime_from_filename(self, timestamp_filename):
        ts = timestamp_filename.lstrip(TIMESTAMP_PREFIX)
        return timeutils.parse_strtime(ts, fmt=TIMESTAMP_FORMAT)

    def _get_ds_browser(self, ds_ref):
        ds_browser = self._ds_browser.get(ds_ref)
        if not ds_browser:
            ds_browser = vim_util.get_dynamic_property(
                    self._session._get_vim(), ds_ref,
                    "Datastore", "browser")
            self._ds_browser[ds_ref] = ds_browser
        return ds_browser

    def _get_base(self, datastore):
        """Returns the base directory of the cached images."""
        ds_path = ds_util.build_datastore_path(datastore['name'],
                                               self._base_folder)
        return ds_path

    def _list_base_images(self, ds_path, datastore):
        """Return a list of the images present in _base.

        This method returns a dictionary with the following keys:
            - unexplained_images
            - originals
        """
        ds_browser = self._get_ds_browser(datastore['ref'])
        originals = ds_util.get_sub_folders(self._session, ds_browser,
                                            ds_path)
        return {'unexplained_images': [],
                'originals': originals}

    def _age_and_verify_cached_images(self, context, datastore, dc_info,
                                      ds_path):
        age_seconds = CONF.remove_unused_original_minimum_age_seconds
        unused_images = self.originals - self.used_images
        ds_browser = self._get_ds_browser(datastore['ref'])
        if unused_images:
            for image in unused_images:
                path = '%s/%s' % (ds_path, image)
                ts = self._get_timestamp(ds_browser, path)
                if not ts:
                    ts_path = '%s/%s' % (path,
                                         self._get_timestamp_filename())
                    ds_util.mkdir(self._session, ts_path, dc_info.ref)
                    LOG.debug(_("Image %s is no longer used by this node. "
                                "Pending deletion!"), image)
                else:
                    dt = self._get_datetime_from_filename(ts)
                    if timeutils.is_older_than(dt, age_seconds):
                        LOG.info(_("Image %s is no longer used. "
                                   "Deleting!"), path)
                        ds_util.file_delete(self._session, None, path,
                                            dc_info.ref)

        # Multi node support - if the image is used and the timestamp file
        # exists then this must be deleted. The timestamp would have been
        # created by another node that no longer has instances running for
        # the specific image.
        if self.used_images:
            for image in self.used_images:
                path = '%s/%s' % (ds_path, image)
                self.timestamp_cleanup(None, dc_info.ref, ds_browser,
                                       datastore['ref'], datastore['name'],
                                       path)

    def update(self, context, instances, datastores_info):
        """The cache manager.

        This will invoke the cache manager. This will update the cache
        according to the defined cache management scheme. The information
        populated in the cached stats will be used for the cache management.
        """
        # read running instances data
        running = self._list_running_instances(context, instances)
        self.used_images = set(running['used_images'].keys())
        # perform the aging and image verification per datastore
        for (datastore, dc_info) in datastores_info:
            ds_path = self._get_base(datastore)
            images = self._list_base_images(ds_path, datastore)
            self.originals = images['originals']
            self._age_and_verify_cached_images(context, datastore, dc_info,
                                               ds_path)
