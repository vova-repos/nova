# Copyright 2014 IBM Corp.
# Copyright 2014 VMware, Inc.
# Copyright 2014 OpenStack Foundation
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

import uuid

import mock

from nova import context
from nova import test
from nova.tests.image import fake as fake_images
from nova.tests.virt.vmwareapi import stubs
from nova.virt import fake
from nova.virt.vmwareapi import driver as vmware_driver
from nova.virt.vmwareapi import fake as vmwareapi_fake
from nova.virt.vmwareapi.imagehandler import copy


class CopyImageHandlerTestCase(test.NoDBTestCase):

    def setUp(self):
        super(CopyImageHandlerTestCase, self).setUp()
        fake_images.stub_out_image_service(self.stubs)
        stubs.set_stubs(self.stubs)
        self.image_meta = {'id': '70a599e0-31e7-49b7-b260-868f441e862b'}
        vmwareapi_fake.reset()
        self.imagehandler = copy.CopyImageHandler(
            vmware_driver.VMwareESXDriver(fake.FakeVirtAPI()))
        self.context = context.RequestContext('fake', 'fake', is_admin=False)

    def tearDown(self):
        super(CopyImageHandlerTestCase, self).tearDown()
        fake_images.FakeImageService_reset()

    def test_fetch_image(self):
        location = {'url': "vsphere://server_host/folder/file_path"
                           "?dcPath=dc_path&dsName=ds_name", 'metadata': ""}
        session = mock.Mock()
        handled = self.imagehandler._fetch_image(
            self.context, self.image_meta.get('id'),
            self.image_meta, '/path',
            datacenter_name='dc1',
            session=session,
            datastore_name='ds1',
            instance_id=str(uuid.uuid4()),
            location=location,
            dst_folder='fake_folder',
            image_fname='cached_name')
        self.assertTrue(handled)

    def test_parse_location_info(self):
        expected_dc_path = 'dc_path'
        expected_ds_name = 'ds_name'
        expected_file_path = '/folder/file'
        location_url = ('vsphere://server_host/folder%s?dcPath=%s&dsName=%s' %
                        (expected_file_path,
                            expected_dc_path,
                            expected_ds_name))
        dc_path, ds_name, file_path = copy._parse_location_info(
            location_url)
        self.assertEqual(file_path, expected_file_path)
        self.assertEqual(dc_path, expected_dc_path)
        self.assertEqual(ds_name, expected_ds_name)

    def test_move_image(self):
        self.assertRaises(NotImplementedError,
                          self.imagehandler._move_image,
                          self.context, self.image_meta.get('id'),
                          self.image_meta, 'source', 'dest')

    def test_remove_image(self):
        self.assertRaises(NotImplementedError,
                          self.imagehandler._remove_image,
                          self.context, self.image_meta.get('id'),
                          self.image_meta, 'path')
