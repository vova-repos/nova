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
Datastore utility functions
"""

import time

from oslo.config import cfg

from nova import exception
from nova.openstack.common.gettextutils import _
from nova.openstack.common import log as logging
from nova.openstack.common import uuidutils
from nova.virt.vmwareapi import vim_util
from nova.virt.vmwareapi import vm_util

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


def build_datastore_path(datastore_name, path):
    """Build the datastore compliant path."""
    return "[%s] %s" % (datastore_name, path)


def split_datastore_path(datastore_path):
    """
    Split the VMware style datastore path to get the Datastore
    name and the entity path.
    """
    spl = datastore_path.split('[', 1)[1].split(']', 1)
    path = ""
    if len(spl) == 1:
        datastore_url = spl[0]
    else:
        datastore_url, path = spl
    return datastore_url, path.strip()


def file_delete(session, instance, datastore_path, dc_ref):
    LOG.debug(_("Deleting the datastore file %s"), datastore_path,
              instance=instance)
    if instance:
        uuid = instance['uuid']
    else:
        uuid = uuidutils.generate_uuid()
    vim = session._get_vim()
    file_delete_task = session._call_method(
            session._get_vim(),
            "DeleteDatastoreFile_Task",
            vim.get_service_content().fileManager,
            name=datastore_path,
            datacenter=dc_ref)
    session._wait_for_task(uuid, file_delete_task)
    LOG.debug(_("Deleted the datastore file"), instance=instance)


def move_folder(session, instance, dc_ref, src_folder, dst_folder):
    LOG.debug(_("Moving folder from %(src)s to %(dst)s."),
              {'src': src_folder, 'dst': dst_folder})
    vim = session._get_vim()
    move_task = session._call_method(
            session._get_vim(),
            "MoveDatastoreFile_Task",
            vim.get_service_content().fileManager,
            sourceName=src_folder,
            sourceDatacenter=dc_ref,
            destinationName=dst_folder,
            destinationDatacenter=dc_ref)
    try:
        session._wait_for_task(instance['uuid'], move_task)
    except Exception as e:
        LOG.warning(_("File moving failed - %s"), e)


def _task_info_state_get(session, search_task):
    retries = 0
    # Wait till the state changes from queued or running.
    # If an error state is returned, it means that the path doesn't exist.
    while True:
        try:
            task_info = session._call_method(vim_util,
                                             "get_dynamic_property",
                                             search_task, "Task", "info")
            if task_info.state in ['queued', 'running']:
                time.sleep(1)
                continue
            return task_info
        except Exception as e:
            LOG.warning(_("Unable to get task_info information - %s"), e)
            retries += 1
            if retries > CONF.vmware.api_retry_count:
                raise exception.ServiceUnavailable()
            time.sleep(1)


def path_exists(session, ds_browser, ds_path):
    """Check if the path exists on the datastore."""
    search_task = session._call_method(
            session._get_vim(),
            "SearchDatastore_Task",
            ds_browser,
            datastorePath=ds_path)
    task_info = _task_info_state_get(session, search_task)
    if task_info.state == 'error':
        return False
    return True


def file_exists(session, ds_browser, ds_path, file_name):
    """Check if the path and file exists on the datastore."""
    client_factory = session._get_vim().client.factory
    search_spec = vm_util.search_datastore_spec(client_factory, file_name)
    search_task = session._call_method(session._get_vim(),
                                             "SearchDatastore_Task",
                                             ds_browser,
                                             datastorePath=ds_path,
                                             searchSpec=search_spec)
    task_info = _task_info_state_get(session, search_task)
    if task_info.state == 'error':
        return False, False

    file_exists = (getattr(task_info.result, 'file', False) and
                   task_info.result.file[0].path == file_name)
    return True, file_exists


def mkdir(session, ds_path, dc_ref):
    """Creates a directory at the path specified. If it is just "NAME",
    then a directory with this name is created at the topmost level of the
    DataStore.
    """
    LOG.debug(_("Creating directory with path %s"), ds_path)
    session._call_method(session._get_vim(), "MakeDirectory",
            session._get_vim().get_service_content().fileManager,
            name=ds_path, datacenter=dc_ref,
            createParentDirectories=True)
    LOG.debug(_("Created directory with path %s"), ds_path)


def get_sub_folders(session, ds_browser, ds_path):
    folders = set()
    if not path_exists(session, ds_browser, ds_path):
        return folders
    client_factory = session._get_vim().client.factory
    search_task = session._call_method(
            session._get_vim(),
            "SearchDatastore_Task",
            ds_browser,
            datastorePath=ds_path)
    task_info = _task_info_state_get(session, search_task)
    # populate the cached images
    if hasattr(task_info.result, 'file'):
        for file in task_info.result.file:
            folders.add(file.path)
    return folders
