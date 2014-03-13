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

import mock

from nova import context
from nova import test
from nova.tests.image import fake as fake_images
from nova.tests.virt.vmwareapi import stubs
from nova.virt import fake
from nova.virt.vmwareapi import driver as vmware_driver
from nova.virt.vmwareapi import fake as vmwareapi_fake
from nova.virt.vmwareapi.imagehandler import download


class DownloadImageHandlerTestCase(test.NoDBTestCase):

    def setUp(self):
        super(DownloadImageHandlerTestCase, self).setUp()
        fake_images.stub_out_image_service(self.stubs)
        stubs.set_stubs(self.stubs)
        self.image_meta = {'id': '70a599e0-31e7-49b7-b260-868f441e862b',
                           'size': 74185822}
        vmwareapi_fake.reset()
        self.driver = vmware_driver.VMwareESXDriver(fake.FakeVirtAPI())
        self.imagehandler = download.DownloadImageHandler(self.driver)
        self.driver._session._call_method = mock.Mock()
        self.driver._session._wait_for_task = mock.Mock()
        self.context = context.RequestContext('fake', 'fake', is_admin=False)

    def tearDown(self):
        super(DownloadImageHandlerTestCase, self).tearDown()
        fake_images.FakeImageService_reset()

    def test_fetch_image(self):
        path = '/path'
        datastore_name = 'ds1'
        session = mock.Mock()
        handled = self.imagehandler._fetch_image(
            self.context, self.image_meta.get('id'),
            self.image_meta, path, datastore_name=datastore_name,
            session=session)
        self.assertTrue(handled)

    def test_push_image(self):
        session = mock.Mock()
        result = self.imagehandler._push_image(
            self.context, self.image_meta.get('id'),
            self.image_meta, 'vmware_temp/fake_uuid-flat.vmdk',
            host='127.0.0.1',
            datacenter_path='dc1',
            datastore_name='ds1',
            session=session)
        self.assertTrue(result[0])

    def test_move_image(self):
        src_path = '/src_path'
        dst_path = '/dst_path'
        datacenter_name = 'dc1'
        session = self.driver._session
        handled = (
            self.imagehandler._move_image(self.context,
                                          self.image_meta.get('id'),
                                          self.image_meta, src_path, dst_path,
                                          session=session,
                                          src_datacenter_name=datacenter_name,
                                          dst_datacenter_name=datacenter_name,
                                          instance_id='fake'))
        self.assertTrue(handled)

    def test_remove_image(self):
        path = '/path'
        datacenter_name = 'dc1'
        session = self.driver._session
        handled = (
            self.imagehandler._remove_image(self.context,
                                            self.image_meta.get('id'),
                                            self.image_meta, path,
                                            session=session,
                                            data_center_name=datacenter_name,
                                            instance_id='fake'))
        self.assertTrue(handled)
