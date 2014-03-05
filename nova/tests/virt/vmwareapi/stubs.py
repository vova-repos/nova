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
Stubouts for the test suite
"""

from oslo.vmware import exceptions as vexc
from oslo.vmware import image_transfer

from nova.virt.vmwareapi import driver
from nova.virt.vmwareapi import fake
from nova.virt.vmwareapi import network_util
from nova.virt.vmwareapi import vmware_images


def fake_get_vim_object(arg):
    """Stubs out the VMwareAPISession's get_vim_object method."""
    return fake.FakeVim()


@property
def fake_vim_prop(arg):
    """Stubs out the VMwareAPISession's vim property access method."""
    return fake.get_fake_vim_object(arg)


def fake_is_vim_object(arg, module):
    """Stubs out the VMwareAPISession's is_vim_object method."""
    return isinstance(module, fake.FakeVim)


def fake_temp_method_exception():
    raise vexc.VimFaultException(
            [vexc.NOT_AUTHENTICATED],
            "Session Empty/Not Authenticated")


def fake_temp_session_exception():
    raise vexc.VimConnectionException("it's a fake!",
            "Session Exception")


def fake_session_file_exception():
    fault_list = [vexc.FILE_ALREADY_EXISTS]
    raise vexc.VimFaultException(fault_list,
                                 Exception('fake'))


def set_stubs(stubs):
    """Set the stubs."""
    stubs.Set(network_util, 'get_network_with_the_name',
              fake.fake_get_network)
    stubs.Set(vmware_images, 'get_vmdk_size_and_properties',
              fake.fake_get_vmdk_size_and_properties)
    stubs.Set(driver.VMwareAPISession, "vim",
              fake_vim_prop)
    stubs.Set(driver.VMwareAPISession, "_is_vim_object",
              fake_is_vim_object)
    stubs.Set(image_transfer, 'download_flat_image',
              fake.fake_download_flat_image)
    stubs.Set(image_transfer, 'download_stream_optimized_image',
              fake.fake_download_stream_optimized_image)
    stubs.Set(image_transfer, 'upload_image', fake.fake_upload_image)
