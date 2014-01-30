# Copyright (c) 2012 VMware, Inc.
# Copyright (c) 2011 Citrix Systems, Inc.
# Copyright 2011 OpenStack Foundation
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
Utility functions for Image transfer.
"""

import contextlib
from lxml import etree
import os
import tarfile
import tempfile

from oslo.vmware import image_transfer

from nova import exception
from nova.image import glance
from nova.openstack.common.gettextutils import _
from nova.openstack.common import log as logging
from nova.virt.vmwareapi import read_write_util

LOG = logging.getLogger(__name__)


def upload_iso_to_datastore(iso_path, instance, **kwargs):
    LOG.debug(_("Uploading iso %s to datastore") % iso_path,
              instance=instance)
    with open(iso_path, 'r') as iso_file:
        write_file_handle = read_write_util.VMwareHTTPWriteFile(
            kwargs.get("host"),
            kwargs.get("data_center_name"),
            kwargs.get("datastore_name"),
            kwargs.get("cookies"),
            kwargs.get("file_path"),
            os.fstat(iso_file.fileno()).st_size)

        LOG.debug(_("Uploading iso of size : %s ") %
                  os.fstat(iso_file.fileno()).st_size)
        block_size = 0x10000
        data = iso_file.read(block_size)
        while len(data) > 0:
            write_file_handle.write(data)
            data = iso_file.read(block_size)
        write_file_handle.close()

    LOG.debug(_("Uploaded iso %s to datastore") % iso_path,
              instance=instance)


def get_disk_info_from_ovf(xmlstr):
    ovf = etree.fromstring(xmlstr)
    nsovf = "{%s}" % ovf.nsmap["ovf"]
    nsrasd = "{%s}" % ovf.nsmap["rasd"]

    disk = ovf.find("./%sDiskSection/%sDisk" % (nsovf, nsovf))
    disk_id = disk.get("%sdiskId" % nsovf)
    file_id = disk.get("%sfileRef" % nsovf)

    file = ovf.find('./%sReferences/%sFile[@%sid="%s"]' % (nsovf, nsovf,
                                                           nsovf, file_id))
    vmdk_name = file.get("%shref" % nsovf)

    hrsrcs = ovf.findall(".//%sHostResource" % (nsrasd))
    hrsrc = [x for x in hrsrcs if x.text == "ovf:/disk/%s" % disk_id][0]
    item = hrsrc.getparent()
    controller_id = item.find("%sParent" % nsrasd).text

    adapter_type = "busLogic"

    instance_nodes = ovf.findall(".//%sItem/%sInstanceID" % (nsovf, nsrasd))
    instance_node = [x for x in instance_nodes if x.text == controller_id][0]
    item = instance_node.getparent()
    desc = item.find("%sDescription" % nsrasd).text
    if desc == "IDE Controller":
        adapter_type = "ide"
    else:
        sub_type = item.find("%sResourceSubType")
        if sub_type:
            adapter_type = sub_type.text

    return (vmdk_name, adapter_type)


def fetch_ova_image(context, timeout_secs, image_service, image_id, **kwargs):
    """Download image from the glance image server."""
    LOG.debug(_("Downloading image %s from glance image server") % image_id)

    read_iter = image_service.download(context, image_id)

    ova_fd, ova_path = tempfile.mkstemp()
    try:
        # Note(vui): Look to eliminate first writing OVA to file system.
        with os.fdopen(ova_fd, 'w') as fp:
            for chunk in read_iter:
                fp.write(chunk)

        with contextlib.closing(tarfile.open(ova_path, mode="r")) as tar:
            vmdk_name = None
            adapter_type = None
            for tar_info in tar:
                if tar_info:
                    if tar_info.name.endswith(".ovf"):
                        extracted = tar.extractfile(tar_info.name)
                        xmlstr = extracted.read()
                        (vmdk_name,
                         adapter_type) = get_disk_info_from_ovf(xmlstr)
                    elif vmdk_name and tar_info.name.startswith(vmdk_name):
                        # Actual file name is <vmdk_name>.XXXXXXX
                        extracted = tar.extractfile(tar_info.name)
                        kwargs["image_size"] = tar_info.size
                        vm = image_transfer.download_stream_optimized_data(
                            context,
                            timeout_secs,
                            extracted,
                            **kwargs)
                        extracted.close()
                        return vm, adapter_type
            raise exception.ImageUnacceptable(
                reason=_("Extracting vmdk from OVA failed."),
                image_id=image_id)
    finally:
        os.unlink(ova_path)


def get_vmdk_size_and_properties(context, image, instance):
    """Get size of the vmdk file that is to be downloaded for attach in spawn.
    Need this to create the dummy virtual disk for the meta-data file. The
    geometry of the disk created depends on the size.
    """

    LOG.debug(_("Getting image size for the image %s") % image,
              instance=instance)
    (image_service, image_id) = glance.get_remote_image_service(context, image)
    meta_data = image_service.show(context, image_id)
    size, container_type, properties = (meta_data["size"],
                                        meta_data["container_format"],
                                        meta_data["properties"])
    LOG.debug(_("Got image size of %(size)s for the image %(image)s"),
              {'size': size, 'image': image}, instance=instance)

    # Container type takes precendence and signifies that the vmdk can
    # only be streamOptimized disk.
    if container_type == 'ovf':
        properties['vmware_disktype'] = "streamOptimized"

    return size, container_type, properties
