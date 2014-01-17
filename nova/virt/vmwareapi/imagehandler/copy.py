# Copyright (c) 2014 VMware, Inc.
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
Copy image handler implementation for the VMware driver.
This implementation requires a vsphere URL meaning that the image is already
on a VMware datastore:
ex: vsphere://server_host/folder/file_path?dcPath=dc_path&dsName=ds_name
"""

import urlparse

from nova import exception
from nova.openstack.common.gettextutils import _
from nova.openstack.common import log as logging
from nova.virt.imagehandler import base
from nova.virt import vmwareapi
from nova.virt.vmwareapi import vm_util

LOG = logging.getLogger(__name__)
DS_URL_PREFIX = '/folder'


def _parse_location_info(location_url):
    """Parse an image location URL to retrieve the datacenter path,
    datastore name and file path. The file path returned is
    not 'datastore-ready'. To be used with the VIM API,
    it needs to be converted (ex: from "/path" to "[ds] /path")

    :param location_url: The URL of the image
    :raises InvalidInput: if it is not possible to parse correctly the
    URL provided.

    :retval the datacenter name, the datastore name, the path of the image.
    """
    (scheme, server_host, file_path, params, query, fragment) = (
        urlparse.urlparse(location_url))
    # src file_path
    if not query:
        file_path = file_path.split('?')
        if len(file_path) > 0:
            query = file_path[1]
            file_path = file_path[0]
        else:
            msg = (_("Location URL %s must contain a file path"), location_url)
            raise exception.InvalidInput(reason=msg)
    if not file_path.startswith(DS_URL_PREFIX):
        msg = (_("Location URL %(url)s must start with %(prefix)s") %
               {'url': location_url, 'prefix': DS_URL_PREFIX})
        raise exception.InvalidInput(reason=msg)
    file_path = file_path[len(DS_URL_PREFIX):]
    # src datacenter name
    params = urlparse.parse_qs(query)
    dc_path = params['dcPath']
    if len(dc_path) > 0:
        dc_path = dc_path.pop()
    else:
        msg = (_("Location URL %(url)s must contain a datacenter path"),
               location_url)
        raise exception.InvalidInput(reason=msg)
    # src datastore
    ds_name = params['dsName']
    if len(ds_name) > 0:
        ds_name = ds_name.pop()
    else:
        msg = (_("Location URL %(url)s must contain a datastore name"),
               location_url)
        raise exception.InvalidInput(reason=msg)
    return dc_path, ds_name, file_path


class CopyImageHandler(base.ImageHandler):

    def __init__(self, driver=None, *args, **kwargs):
        applicable_drivers = [vmwareapi.VMwareESXDriver,
                              vmwareapi.VMwareVCDriver]
        if driver is None or type(driver) not in applicable_drivers:
            msg = (_("Can't handle images of driver: %s") % driver)
            LOG.warn(msg)
            raise Exception(msg)
        super(CopyImageHandler, self).__init__(driver, *args, **kwargs)

    def get_schemes(self):
        return ('vsphere')

    def is_local(self):
        return True

    def _fetch_image(self, context, image_id, image_meta, path,
                     user_id=None, project_id=None, location=None,
                     **kwargs):
        LOG.debug(_("Fetching image %s from glance image server"), image_id)

        # Sanity check
        session = kwargs.get('session')
        if session is None:
            LOG.error(_("Cannot fetch image %s with null session") % image_id)
            return
        cache_folder = kwargs.get('cache_folder')
        if cache_folder is None:
            LOG.error(_("Cannot fetch image %s with null cache folder")
                      % image_id)
            return
        cached_fname = kwargs.get('cached_fname')
        if cached_fname is None:
            LOG.error(_("Cannot fetch image % with null cache filename")
                      % image_id)
            return
        dst_dc_path = kwargs.get("datacenter_name")
        if dst_dc_path is None:
            LOG.error(_("Cannot fetch image %s with null datacenter path") %
                      image_id)
            return
        dst_ds_name = kwargs.get('datastore_name')
        if dst_ds_name is None:
            LOG.error(_("Cannot fetch image %s with null datastore name") %
                      image_id)
            return
        disk_type = kwargs.get('disk_type')
        if disk_type is None:
            LOG.error(_("Cannot fetch image %s with null disk type")
                      % disk_type)
            return
        vmdk_file_size_in_kb = kwargs.get('vmdk_file_size_in_kb')
        if vmdk_file_size_in_kb is None:
            LOG.error(_('Cannot fetch image %s with null file size')
                      % image_id)
            return
        adapter_type = kwargs.get('adapter_type')
        if adapter_type is None:
            LOG.error(_('Cannot fetch image %s with null adapter type')
                      % image_id)
            return
        thin_copy = kwargs.get('thin_copy')
        if thin_copy is None:
            LOG.error(_('Cannot fetch image %s with null thin copy flag')
                      % image_id)
            return

        # Retrieve information from image location
        try:
            src_dc_path, src_ds_name, src_file_path = (
                _parse_location_info(location.get('url')))
        except Exception:
            LOG.error(_("Unable parse location url %(url)s for "
                        "image %(image)s") % {'url': location.get('url'),
                                              'image': image_id})
            return
        if src_dc_path is None or src_ds_name is None or src_file_path is None:
            LOG.error(_("Cannot fetch image %(image)s with datacenter "
                        "%(dc_path)s, datastore %(ds_name)s and file location "
                        "%(file)s") % {'image': image_id,
                                       'dc_path': src_dc_path,
                                       'ds_name': src_ds_name,
                                       'file': src_file_path})
            return
        service_content = session._get_vim().retrieve_service_content()
        search_index_moref = service_content.searchIndex
        src_moref = session._call_method(session._get_vim(),
                                         'FindByInventoryPath',
                                         search_index_moref,
                                         inventoryPath=src_dc_path)
        if src_moref is None:
            LOG.error(_("Unable to find the moref for datacenter %s"),
                      src_dc_path)
            return

        dst_moref = session._call_method(session._get_vim(),
                                          'FindByInventoryPath',
                                          search_index_moref,
                                          inventoryPath=dst_dc_path)
        if dst_moref is None:
            LOG.error(_("Unable to find the moref for datacenter %s"),
                      dst_dc_path)
            return

        src_file_path = '[%s] %s' % (src_ds_name, src_file_path)

        # Create directory for the image in the cache
        cache_ds_dir = '[%s] %s/%s' % (dst_ds_name, cache_folder, image_id)
        cache_file_ds_path = '%s/%s' % (cache_ds_dir, cached_fname)

        session._call_method(session._get_vim(),
                             "MakeDirectory",
                             service_content.fileManager,
                             name=cache_ds_dir,
                             datacenter=dst_moref,
                             createParentDirectories=True)

        if disk_type != "sparse":
            # Create a flat virtual disk (*-flat.vmdk + *.vmdk) and
            # retain the descriptor file (*.vmdk) in the temp directory.
            descriptor_ds_path = ('%s.vmdk' % cache_file_ds_path[:-10])
            client_factory = session._get_vim().client.factory

            vmdk_create_spec = vm_util.get_vmdk_create_spec(
                client_factory, vmdk_file_size_in_kb, adapter_type, disk_type)

            vmdk_create_task = session._call_method(
                session._get_vim(),
                "CreateVirtualDisk_Task",
                service_content.virtualDiskManager,
                name=descriptor_ds_path,
                datacenter=dst_moref,
                spec=vmdk_create_spec)
            session._wait_for_task(None, vmdk_create_task)

            file_delete_task = session._call_method(
                session._get_vim(),
                "DeleteDatastoreFile_Task",
                service_content.fileManager,
                name=cache_file_ds_path,
                datacenter=dst_moref)
            session._wait_for_task(None, file_delete_task)

        # Copy the image to the cache directory
        copy_task = session._call_method(
            session._get_vim(),
            "CopyDatastoreFile_Task",
            service_content.fileManager,
            sourceName=src_file_path,
            sourceDatacenter=src_moref,
            destinationName=cache_file_ds_path,
            destinationDatacenter=dst_moref,
            force=True)
        try:
            session._wait_for_task(None, copy_task)
        except Exception as exc:
            LOG.warn(_("Failed to move image %(image)s: %(exc)s") %
                     {'image': image_id, 'exc': exc})
        LOG.debug(_("Fetched image %s from glance image server"), image_id)
        return cache_file_ds_path

    def _remove_image(self, context, image_id, image_meta, path,
                      user_id=None, project_id=None, location=None,
                      **kwargs):
        raise NotImplementedError()

    def _move_image(self, context, image_id, image_meta, src_path, dst_path,
                    user_id=None, project_id=None, location=None,
                    **kwargs):
        raise NotImplementedError()
