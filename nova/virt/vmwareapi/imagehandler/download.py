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
Download image handler implementation for the VMware driver.
"""

from nova.image import glance
from nova.openstack.common.gettextutils import _
from nova.openstack.common import log as logging
from nova.virt.imagehandler import base
from nova.virt import vmwareapi
from nova.virt.vmwareapi import read_write_util
from nova.virt.vmwareapi import vmware_images

LOG = logging.getLogger(__name__)


class DownloadImageHandler(base.ImageHandler):

    def __init__(self, driver=None, *args, **kwargs):
        applicable_drivers = [vmwareapi.VMwareESXDriver,
                              vmwareapi.VMwareVCDriver]
        if driver is None or type(driver) not in applicable_drivers:
            msg = _("Can't handle images of driver: %s") % driver
            LOG.warn(msg)
            raise Exception(msg)
        super(DownloadImageHandler, self).__init__(driver, *args, **kwargs)

    def get_schemes(self):
        return ()

    def is_local(self):
        return False

    def _fetch_image(self, context, image_id, image_meta, path,
                     user_id=None, project_id=None, location=None,
                     **kwargs):
        LOG.debug(_("Fetching image %s from glance image server"), image_id)
        (image_service, image_id) = glance.get_remote_image_service(context,
                                                                    image_id)
        file_size = int(image_meta.get('size'))
        try:
            read_iter = image_service.download(context, image_id)
            read_file_handle = read_write_util.GlanceFileRead(read_iter)
            write_file_handle = read_write_util.VMwareHTTPWriteFile(
                kwargs.get("host"),
                kwargs.get("data_center_name"),
                kwargs.get("datastore_name"),
                kwargs.get("cookies"),
                path,
                file_size)
            vmware_images.start_transfer(context, read_file_handle, file_size,
                                         write_file_handle=write_file_handle)
        except Exception as exc:
            LOG.error(_("Failed to fetch image %(image)s: %(exc)s"),
                      {'image': image_id, 'exc': exc})
            return False
        LOG.debug(_("Fetched image %s from glance image server"), image_id)
        return True

    def _remove_image(self, context, image_id, image_meta, path,
                      user_id=None, project_id=None, location=None,
                      **kwargs):
        session = kwargs.get('session')
        if session is None:
            LOG.error(_("Cannot remove image %s with null session"), image_id)
            return False
        dc_path = kwargs.get("data_center_name")
        if dc_path is None:
            LOG.error(_("Cannot remove image %s with null datacenter path"),
                      image_id)
            return False
        instance_id = kwargs.get("instance_id")
        service_content = session._get_vim().get_service_content()

        dc_moref = session._call_method(session._get_vim(),
                                        'FindByInventoryPath',
                                        service_content.searchIndex,
                                        inventoryPath=dc_path)
        if dc_moref is None:
            LOG.error(_("Unable to find the moref for datacenter %s"),
                      dc_path)
            return False
        delete_task = session._call_method(session._get_vim(),
                                           'DeleteDatastoreFile_Task',
                                           service_content.fileManager,
                                           name=path,
                                           datacenter=dc_moref)
        try:
            session._wait_for_task(instance_id, delete_task)
        except Exception as exc:
            LOG.error(_("Failed to remove image %(image)s: %(exc)s"),
                      {'image': image_id, 'exc': exc})
            return False
        LOG.debug(_("Removed image %(image)s from path %(path)s"),
                  {'image': image_id, 'path': path})
        return True

    def _move_image(self, context, image_id, image_meta, src_path, dst_path,
                    user_id=None, project_id=None, location=None,
                    **kwargs):
        session = kwargs.get('session')
        if session is None:
            LOG.error(_("Cannot remove image %s with null session"), image_id)
            return False
        src_dc_path = kwargs.get("src_datacenter_name")
        if src_dc_path is None:
            LOG.error(_("Cannot remove image %s with null source "
                        "datacenter path"), image_id)
            return False
        dst_dc_path = kwargs.get("dst_datacenter_name")
        if dst_dc_path is None:
            LOG.error(_("Cannot remove image %s with null destination "
                        "datacenter path"), image_id)
            return False
        instance_id = kwargs.get("instance_id")
        service_content = session._get_vim().get_service_content()
        src_dc_moref = session._call_method(session._get_vim(),
                                            'FindByInventoryPath',
                                            service_content.searchIndex,
                                            inventoryPath=src_dc_path)
        if src_dc_moref is None:
            LOG.error(_("Unable to find the moref for datacenter %s"),
                      src_dc_path)
            return False

        dst_dc_moref = session._call_method(session._get_vim(),
                                            'FindByInventoryPath',
                                            service_content.searchIndex,
                                            inventoryPath=dst_dc_path)
        if dst_dc_moref is None:
            LOG.error(_("Unable to find the moref for datacenter %s"),
                      dst_dc_path)
            return False
        move_task = session._call_method(session._get_vim(),
                                         'MoveDatastoreFile_Task',
                                         service_content.fileManager,
                                         sourceName=src_path,
                                         sourceDatacenter=src_dc_moref,
                                         destinationName=dst_path,
                                         destinationDatacenter=
                                         dst_dc_moref)
        try:
            session._wait_for_task(instance_id, move_task)
        except Exception as exc:
            LOG.error(_("Failed to move image %(image)s: %(exc)s"),
                      {'image': image_id, 'exc': exc})
            return False
        LOG.debug(_("Moved image %(image)s from %(src_path)s to %(dst_path)s"),
                  {'image': image_id, 'src_path': src_path,
                   'dst_path': dst_path})
        return True
