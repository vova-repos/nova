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
Exception classes and SOAP response error checking module.
"""
from oslo.vmware import exceptions as vexc

from nova.openstack.common.gettextutils import _
from nova.openstack.common import log as logging

LOG = logging.getLogger(__name__)

# Most VMware-specific exception classes are now centrally defined in
# oslo.vmware.
# Note(vui):
# - map back to NovaException?
# - 1c6898efecd7d78855efcb22d9c38d6127339000 should be ported oslo.vmware


class VMwareDriverConfigurationException(vexc.VMwareDriverException):
    """Base class for all configuration exceptions.
    """
    msg_fmt = _("VMware Driver configuration fault.")


class UseLinkedCloneConfigurationFault(VMwareDriverConfigurationException):
    msg_fmt = _("No default value for use_linked_clone found.")


class PbmDefaultPolicyDoesNotExist(VMwareDriverConfigurationException):
    msg_fmt = _("Default PBM policy is not defined.")


class FaultCheckers(object):
    """Methods for fault checking of SOAP response. Per Method error handlers
    for which we desire error checking are defined. SOAP faults are
    embedded in the SOAP messages as properties and not as SOAP faults.
    """

    @staticmethod
    def retrievepropertiesex_fault_checker(resp_obj):
        """Checks the RetrievePropertiesEx response for errors. Certain faults
        are sent as part of the SOAP body as property of missingSet.
        For example NotAuthenticated fault.
        """
        fault_list = []
        if not resp_obj:
            # This is the case when the session has timed out. ESX SOAP server
            # sends an empty RetrievePropertiesResponse. Normally missingSet in
            # the returnval field has the specifics about the error, but that's
            # not the case with a timed out idle session. It is as bad as a
            # terminated session for we cannot use the session. So setting
            # fault to NotAuthenticated fault.
            fault_list = [vexc.NOT_AUTHENTICATED]
        else:
            for obj_cont in resp_obj.objects:
                if hasattr(obj_cont, "missingSet"):
                    for missing_elem in obj_cont.missingSet:
                        fault_type = missing_elem.fault.fault.__class__
                        # Fault needs to be added to the type of fault for
                        # uniformity in error checking as SOAP faults define
                        fault_list.append(fault_type.__name__)
        if fault_list:
            exc_msg_list = ', '.join(fault_list)
            raise vexc.VimFaultException(fault_list, Exception(_("Error(s) %s "
                    "occurred in the call to RetrievePropertiesEx") %
                    exc_msg_list))
