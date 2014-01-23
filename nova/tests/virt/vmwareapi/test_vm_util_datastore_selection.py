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

import collections
import re

import mock

from oslo.vmware import pbm

from nova.openstack.common import units

from nova import test
from nova.virt.vmwareapi import fake
from nova.virt.vmwareapi import vm_util

ResultSet = collections.namedtuple('ResultSet', ['objects'])
ResultSetToken = collections.namedtuple('ResultSet', ['objects', 'token'])
ObjectContent = collections.namedtuple('ObjectContent', ['obj', 'propSet'])
DynamicProperty = collections.namedtuple('Property', ['name', 'val'])
MoRef = collections.namedtuple('ManagedObjectReference', ['value'])


class fake_session(object):
    def __init__(self, ret=None):
        self.ret = ret
        self.pbm = fake.FakeVim()

    def _call_method(self, *args):
        return self.ret


class VMwareVMUtilDatastoreSelectionTestCase(test.NoDBTestCase):

    def setUp(self):
        super(VMwareVMUtilDatastoreSelectionTestCase, self).setUp()
        self.data = [
            ['VMFS', 'os-some-name', True, 987654321, 12346789],
            ['NFS', 'another-name', True, 9876543210, 123467890],
            ['BAD', 'some-name-bad', True, 98765432100, 1234678900],
            ['VMFS', 'some-name-good', False, 987654321, 12346789],
        ]

    def build_result_set(self, mock_data, name_list=None):
        # datastores will have a moref_id of ds-000 and
        # so on based on their index in the mock_data list
        if name_list is None:
            name_list = self.propset_name_list

        objects = []
        for id, row in enumerate(mock_data):
            obj = ObjectContent(
                obj=MoRef(value="ds-%03d" % id),
                propSet=[])
            for index, value in enumerate(row):
                obj.propSet.append(
                    DynamicProperty(name=name_list[index], val=row[index]))
            objects.append(obj)
        return ResultSet(objects=objects)

    @property
    def propset_name_list(self):
        return ['summary.type', 'summary.name', 'summary.accessible',
                'summary.capacity', 'summary.freeSpace']

    def test_filter_datastores_simple(self):
        datastores = self.build_result_set(self.data)
        best_match = vm_util.DSRecord(
            datastore=None, name=None, capacity=None, freespace=0, type=None,
            accessible=True)
        rec = vm_util._select_datastore(datastores, best_match)

        self.assertIsNotNone(rec[0], "could not find datastore!")
        self.assertEqual('ds-001', rec[0].value,
                         "didn't find the right datastore!")
        self.assertEqual(123467890, rec[3],
                         "did not obtain correct freespace!")

    def test_filter_datastores_empty(self):
        data = []
        datastores = self.build_result_set(data)

        best_match = vm_util.DSRecord(
            datastore=None, name=None, type=None, capacity=None, freespace=0,
            accessible=False)
        rec = vm_util._select_datastore(datastores, best_match)

        self.assertEqual(rec, best_match)

    def test_filter_datastores_no_match(self):
        datastores = self.build_result_set(self.data)
        datastore_regex = re.compile('no_match.*')

        best_match = vm_util.DSRecord(
            datastore=None, name=None, type=None, capacity=None, freespace=0,
            accessible=False)
        rec = vm_util._select_datastore(datastores,
                                        best_match,
                                        datastore_regex)

        self.assertEqual(rec, best_match, "did not match datastore properly")

    def test_filter_datastores_specific_match(self):

        data = [
            ['VMFS', 'os-some-name', True, 987654321, 1234678],
            ['NFS', 'another-name', True, 9876543210, 123467890],
            ['BAD', 'some-name-bad', True, 98765432100, 1234678900],
            ['VMFS', 'some-name-good', True, 987654321, 12346789],
            ['VMFS', 'some-other-good', False, 987654321000, 12346789000],
            ['vsan', 'some-vsan-good', True, 987654321, 123467890],
        ]
        # only the DS some-name-good is accessible and matches the regex
        datastores = self.build_result_set(data)
        datastore_regex = re.compile('.*-good$')

        best_match = vm_util.DSRecord(
            datastore=None, name=None, type=None, capacity=None, freespace=0,
            accessible=False)
        rec = vm_util._select_datastore(datastores,
                                        best_match,
                                        datastore_regex,
                                        None, None,
                                        vm_util.NON_STREAMABLE_DS_TYPES)

        self.assertIsNotNone(rec, "could not find datastore!")
        self.assertEqual('ds-003', rec[0].value,
                         "didn't find the right datastore!")
        self.assertNotEqual('ds-004', rec[0].value,
                            "accepted an unreachable datastore!")
        self.assertEqual('some-name-good', rec[1])
        self.assertEqual(12346789, rec[3],
                         "did not obtain correct freespace!")
        self.assertEqual(987654321, rec[2],
                         "did not obtain correct capacity!")

        rec = vm_util._select_datastore(datastores,
                                        best_match,
                                        datastore_regex,
                                        None, None,
                                        vm_util.ALL_SUPPORTED_DS_TYPES)

        self.assertIsNotNone(rec, "could not find datastore!")
        self.assertEqual('ds-005', rec[0].value,
                         "didn't find the right datastore!")
        self.assertNotEqual('ds-003', rec[0].value,
                            "accepted an unreachable datastore!")
        self.assertEqual('some-vsan-good', rec[1])
        self.assertEqual(123467890, rec[3],
                         "did not obtain correct freespace!")
        self.assertEqual(987654321, rec[2],
                         "did not obtain correct capacity!")

    def test_filter_datastores_missing_props(self):
        data = [
            ['VMFS', 'os-some-name', 987654321, 1234678],
            ['NFS', 'another-name', 9876543210, 123467890],
        ]
        # no matches are expected when 'summary.accessible' is missing
        prop_names = ['summary.type', 'summary.name',
                      'summary.capacity', 'summary.freeSpace']
        datastores = self.build_result_set(data, prop_names)
        best_match = vm_util.DSRecord(
            datastore=None, name=None, capacity=None, freespace=None,
            type=None, accessible=False)

        rec = vm_util._select_datastore(datastores, best_match)
        self.assertEqual(rec, best_match, "no matches were expected")

    def test_filter_datastores_best_match(self):
        data = [
            ['VMFS', 'spam-good', True, 20 * units.Gi, 10 * units.Gi],
            ['NFS', 'eggs-good', True, 40 * units.Gi, 15 * units.Gi],
            ['BAD', 'some-name-bad', True, 30 * units.Gi, 20 * units.Gi],
            ['VMFS', 'some-name-good', True, 50 * units.Gi, 5 * units.Gi],
            ['VMFS', 'some-other-good', True, 10 * units.Gi, 10 * units.Gi],
        ]

        datastores = self.build_result_set(data)
        datastore_regex = re.compile('.*-good$')

        # the current best match is better than all candidates
        best_match = vm_util.DSRecord(
            datastore='ds-100', name='best-ds-good', type='VMFS',
            capacity=20 * units.Gi, freespace=19 * units.Gi, accessible=True)
        rec = vm_util._select_datastore(datastores,
                                        best_match,
                                        datastore_regex)
        self.assertEqual(rec, best_match, "did not match datastore properly")

    def test_is_valid_ds_record(self):
        data = [
            ['VMFS', 'spam-good', True, 20 * units.Gi, 10 * units.Gi],
            ['NFS', 'eggs-good', True, 40 * units.Gi, 15 * units.Gi],
            ['CFS', 'some-name-good', True, 50 * units.Gi, 5 * units.Gi],
            ['NFS', 'some-other-good', False, 10 * units.Gi, 10 * units.Gi],
        ]
        validated_recs = [vm_util._is_valid_ds_record(vm_util.DSRecord(
            datastore='ds-100', type=row[0], name=row[1], accessible=row[2],
            capacity=row[3], freespace=row[4])) for row in data]
        self.assertEqual([True, True, False, False], validated_recs)

    @mock.patch.object(pbm, 'get_profile_id_by_name')
    @mock.patch.object(pbm, 'filter_hubs_by_profile')
    def test_datastores_matching_storage_policy(self, fake_matching_hubs,
                                                fake_profile_id):
        fake_profile_id.return_value = 'fake_profile_id'
        datastores = fake.FakeRetrieveResult()
        datastores.add_object(fake.Datastore("fake-ds0"))
        datastores.add_object(fake.Datastore("fake-ds1"))
        datastores.add_object(fake.Datastore("fake-ds2"))
        datastores.add_object(fake.Datastore("fake-ds3"))

        # Use only datastores ds2, ds3
        expected_datastores = fake.FakeRetrieveResult()
        expected_datastores.objects = datastores.objects[2:]
        # Convert expected_datastores to hubs
        ds_mors = [oc.obj for oc in expected_datastores.objects]
        fake_hubs = pbm.convert_datastores_to_hubs(fake.FakeFactory(),
                                                        ds_mors)
        fake_matching_hubs.return_value = fake_hubs

        # Only the datastores matching fake hubs should be returned
        result = vm_util._select_datastores_matching_storage_policy(
            fake_session(), datastores, 'fake_policy')
        self.assertEqual(expected_datastores.objects, result.objects)

    @mock.patch.object(pbm, 'get_profile_id_by_name')
    @mock.patch.object(pbm, 'filter_hubs_by_profile', return_value=None)
    def test_no_datastores_matching_storage_policy(self, fake_matching_hubs,
                                                   fake_profile_id):
        fake_profile_id.return_value = 'fake_profile_id'
        datastores = fake.FakeRetrieveResult()
        datastores.add_object(fake.Datastore("fake-ds0"))
        datastores.add_object(fake.Datastore("fake-ds1"))

        # Only the datastores matching fake hubs should be returned
        result = vm_util._select_datastores_matching_storage_policy(
            fake_session(), datastores, 'fake_policy')
        self.assertIsNone(result)
