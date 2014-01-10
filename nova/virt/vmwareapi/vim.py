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
Classes for making VMware VI SOAP calls.
"""

from oslo.config import cfg
import suds

vmwareapi_wsdl_loc_opt = cfg.StrOpt('wsdl_location',
        help='Optional VIM Service WSDL Location '
             'e.g http://<server>/vimService.wsdl. '
             'Optional over-ride to default location for bug work-arounds')

CONF = cfg.CONF
CONF.register_opt(vmwareapi_wsdl_loc_opt, 'vmware')


def get_moref(value, type):
    """Get managed object reference."""
    moref = suds.sudsobject.Property(value)
    moref._type = type
    return moref


def object_to_dict(obj, list_depth=1):
    """Convert Suds object into serializable format.

    The calling function can limit the amount of list entries that
    are converted.
    """
    d = {}
    for k, v in suds.sudsobject.asdict(obj).iteritems():
        if hasattr(v, '__keylist__'):
            d[k] = object_to_dict(v, list_depth=list_depth)
        elif isinstance(v, list):
            d[k] = []
            used = 0
            for item in v:
                used = used + 1
                if used > list_depth:
                    break
                if hasattr(item, '__keylist__'):
                    d[k].append(object_to_dict(item, list_depth=list_depth))
                else:
                    d[k].append(item)
        else:
            d[k] = v
    return d
