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

import contextlib
import mock

from nova import exception
from nova import test
from nova.virt.vmwareapi import ds_util
from nova.virt.vmwareapi import fake


class fake_session(object):
    def __init__(self, ret=None):
        self.ret = ret

    def _get_vim(self):
        return fake.FakeVim()

    def _call_method(self, module, method, *args, **kwargs):
        return self.ret

    def _wait_for_task(self, instance_uuid, task_ref):
        return ''


class DsUtilTestCase(test.NoDBTestCase):
    def setUp(self):
        super(DsUtilTestCase, self).setUp()
        self.session = fake_session()
        self.instance = {'uuid': 'fake-uuid'}
        self.flags(api_retry_count=1, group='vmware')
        fake.reset()

    def tearDown(self):
        super(DsUtilTestCase, self).tearDown()
        fake.reset()

    def test_build_datastore_path(self):
        path = ds_util.build_datastore_path('ds', 'folder')
        self.assertEqual('[ds] folder', path)
        path = ds_util.build_datastore_path('ds', 'folder/file')
        self.assertEqual('[ds] folder/file', path)

    def test_split_datastore_path(self):
        url, path = ds_util.split_datastore_path('[ds]')
        self.assertEqual('ds', url)
        self.assertEqual('', path)
        url, path = ds_util.split_datastore_path('[ds] folder')
        self.assertEqual('ds', url)
        self.assertEqual('folder', path)
        url, path = ds_util.split_datastore_path('[ds] folder/file')
        self.assertEqual('ds', url)
        self.assertEqual('folder/file', path)

    def test_file_delete(self):
        def fake_call_method(module, method, *args, **kwargs):
            self.assertEqual('DeleteDatastoreFile_Task', method)
            name = kwargs.get('name')
            self.assertEqual('fake-datastore-path', name)
            datacenter = kwargs.get('datacenter')
            self.assertEqual('fake-dc-ref', datacenter)
            return 'fake_delete_task'

        with contextlib.nested(
            mock.patch.object(self.session, '_wait_for_task'),
            mock.patch.object(self.session, '_call_method',
                              fake_call_method)
        ) as (_wait_for_task, _call_method):
            ds_util.file_delete(self.session, self.instance,
                                'fake-datastore-path', 'fake-dc-ref')
            _wait_for_task.assert_has_calls([
                   mock.call(self.instance['uuid'], 'fake_delete_task')])

    def test_move_folder(self):
        def fake_call_method(module, method, *args, **kwargs):
            self.assertEqual('MoveDatastoreFile_Task', method)
            sourceName = kwargs.get('sourceName')
            self.assertEqual('[ds] tmp/src', sourceName)
            destinationName = kwargs.get('destinationName')
            self.assertEqual('[ds] base/dst', destinationName)
            sourceDatacenter = kwargs.get('sourceDatacenter')
            self.assertEqual('fake-dc-ref', sourceDatacenter)
            destinationDatacenter = kwargs.get('destinationDatacenter')
            self.assertEqual('fake-dc-ref', destinationDatacenter)
            return 'fake_move_task'

        with contextlib.nested(
            mock.patch.object(self.session, '_wait_for_task'),
            mock.patch.object(self.session, '_call_method',
                              fake_call_method)
        ) as (_wait_for_task, _call_method):
            ds_util.move_folder(self.session, self.instance,
                                'fake-dc-ref', '[ds] tmp/src', '[ds] base/dst')
            _wait_for_task.assert_has_calls([
                   mock.call(self.instance['uuid'], 'fake_move_task')])

    def test_path_exists(self):
        def fake_call_method(module, method, *args, **kwargs):
            if method == 'SearchDatastore_Task':
                ds_browser = args[0]
                self.assertEqual('fake-browser', ds_browser)
                datastorePath = kwargs.get('datastorePath')
                self.assertEqual('fake-path', datastorePath)
                return 'fake_exists_task'
            elif method == 'get_dynamic_property':
                info = fake.DataObject()
                info.name = 'search_task'
                info.state = 'success'
                return info
            # Should never get here
            self.assertTrue(False)

        with mock.patch.object(self.session, '_call_method',
                               fake_call_method):
            res = ds_util.path_exists(self.session, 'fake-browser',
                                      'fake-path')
            self.assertTrue(res)

    def test_path_exists_fails(self):
        def fake_call_method(module, method, *args, **kwargs):
            if method == 'SearchDatastore_Task':
                return 'fake_exists_task'
            elif method == 'get_dynamic_property':
                info = fake.DataObject()
                info.name = 'search_task'
                info.state = 'error'
                return info
            # Should never get here
            self.assertTrue(False)

        with mock.patch.object(self.session, '_call_method',
                               fake_call_method):
            res = ds_util.path_exists(self.session, 'fake-browser',
                                      'fake-path')
            self.assertFalse(res)

    def test_mkdir(self):
        def fake_call_method(module, method, *args, **kwargs):
            self.assertEqual('MakeDirectory', method)
            name = kwargs.get('name')
            self.assertEqual('fake-path', name)
            datacenter = kwargs.get('datacenter')
            self.assertEqual('fake-dc-ref', datacenter)
            createParentDirectories = kwargs.get('createParentDirectories')
            self.assertTrue(createParentDirectories)

        with mock.patch.object(self.session, '_call_method',
                               fake_call_method):
            ds_util.mkdir(self.session, 'fake-path', 'fake-dc-ref')

    def test_task_info_state_get(self):
        def fake_call_method(module, method, *args, **kwargs):
            self.assertEqual('get_dynamic_property', method)
            if not self.incomplete:
                info = fake.DataObject()
                info.name = 'search_task'
                info.state = 'success'
                return info
            else:
                raise 'fake exception'

        with mock.patch.object(self.session, '_call_method',
                               fake_call_method):
            self.incomplete = False
            res = ds_util._task_info_state_get(self.session,
                                               'fake-search-task')
            self.assertEqual('success', res.state)
            self.incomplete = True
            self.assertRaises(exception.ServiceUnavailable,
                    ds_util._task_info_state_get, self.session,
                    'fake-search-task')

    def test_file_exists(self):
        def fake_call_method(module, method, *args, **kwargs):
            if method == 'SearchDatastore_Task':
                ds_browser = args[0]
                self.assertEqual('fake-browser', ds_browser)
                datastorePath = kwargs.get('datastorePath')
                self.assertEqual('fake-path', datastorePath)
                return 'fake_exists_task'
            elif method == 'get_dynamic_property':
                info = fake.DataObject()
                info.name = 'search_task'
                info.state = 'success'
                result = fake.DataObject()
                result.path = 'fake-path'
                matched = fake.DataObject()
                matched.path = 'fake-file'
                result.file = [matched]
                info.result = result
                return info
            # Should never get here
            self.assertTrue(False)

        with mock.patch.object(self.session, '_call_method',
                               fake_call_method):
            task, file = ds_util.file_exists(self.session, 'fake-browser',
                                             'fake-path', 'fake-file')
            self.assertTrue(task)
            self.assertTrue(file)

    def test_file_exists_fails(self):
        def fake_call_method(module, method, *args, **kwargs):
            if method == 'SearchDatastore_Task':
                return 'fake_exists_task'
            elif method == 'get_dynamic_property':
                info = fake.DataObject()
                info.name = 'search_task'
                info.state = 'error'
                return info
            # Should never get here
            self.assertTrue(False)

        with mock.patch.object(self.session, '_call_method',
                               fake_call_method):
            task, file = ds_util.file_exists(self.session, 'fake-browser',
                                             'fake-path', 'fake-file')
            self.assertFalse(task)
            self.assertFalse(file)
