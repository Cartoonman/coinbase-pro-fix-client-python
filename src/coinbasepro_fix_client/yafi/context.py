#!/usr/bin/env python
from pathlib import Path
import xmltodict
from collections import OrderedDict

import os

_ROOT = os.path.abspath(os.path.dirname(__file__))


class FIXContext(object):
    def __init__(
        self, protocol_descriptor, input_mode="quickfix"
    ):  # TODO split all dictionaries into id/name keys, then merge them into main dict for each.
        self.version = None
        self._protocol_tags = OrderedDict()  # name: {id, type} / id: {name, type}
        self._protocol_msgs = OrderedDict()  # admin: {}, app: {}
                                  # name: {msgtype, tags_name: {name: req}, tags_id: {id: req}} / msgtype: {name, tags_name: {name: req}, tags_id: {id: req}}
        self._protocol_header = OrderedDict()  # name: {name: req} / n_id: {id: req}
        self._protocol_trailer = OrderedDict()  # name: {name: req} / n_id: {id: req}
        self.initialized = False
        impl_funcs = {"quickfix": self._quickfix_xml_loader}

        try:
            impl_funcs[input_mode](protocol_descriptor)
        except KeyError:
            print(
                f"Error: {input_mode} has not been implemented yet. Existing implementations: {[i for i in impl_funcs.keys()]}"
            )

    def _quickfix_xml_loader(self, protocol_descriptor):
        protocol_file = Path(
            os.path.join(_ROOT, "fix_protocol", protocol_descriptor + ".xml")
        )
        try:
            with protocol_file.open() as file:
                protocol_dict = xmltodict.parse(file.read())["fix"]
                # Extract version
                self.version = (
                    protocol_dict["@type"]
                    + "."
                    + protocol_dict["@major"]
                    + "."
                    + protocol_dict["@minor"]
                )

                protocol_tags_name = {}
                protocol_tags_num = {}
                for tag in protocol_dict["fields"]["field"]:
                    protocol_tags_name[tag["@name"]] = {
                        "n_id": int(tag["@number"]),
                        "d_type": tag["@type"],
                    }
                    protocol_tags_num[int(tag["@number"])] = {
                        "name": tag["@name"],
                        "d_type": tag["@type"],
                    }
                    self._protocol_tags.update(protocol_tags_name)
                    self._protocol_tags.update(protocol_tags_num)

                protocol_msgs_admin_name = {}
                protocol_msgs_admin_msgtype = {}
                protocol_msgs_app_name = {}
                protocol_msgs_app_msgtype = {}
                self._protocol_msgs['admin'] = {}
                self._protocol_msgs['app'] = {}
                for message in protocol_dict["messages"]["message"]:
                    tags_name = {}
                    tags_id = {}
                    if isinstance(message["field"], list):
                        for tag in message["field"]:
                            tags_name[tag["@name"]] = {'required': tag["@required"]}
                            tags_id[self._protocol_tags[tag["@name"]]['n_id']] = {'required': tag["@required"]}
                    else:
                        tags_name[message["field"]["@name"]] = {'required': message["field"]["@required"]} 
                        tags_id[self._protocol_tags[message["field"]["@name"]]['n_id']] = {'required': message["field"]["@required"]}

                    try:
                        if isinstance(message["group"], list):
                            for group in message["group"]:
                                group_dict = self._recurse_groups(group, False)
                                tags_name.update(group_dict)
                                group_dict = self._recurse_groups(group, True)
                                tags_id.update(group_dict)
                        else:
                            group_dict = self._recurse_groups(message["group"], False)
                            tags_name.update(group_dict)
                            group_dict = self._recurse_groups(message["group"], True)
                            tags_id.update(group_dict)
                    except KeyError:
                        pass
                    if message["@msgcat"] == "admin":
                        protocol_msgs_admin_msgtype[message["@msgtype"]] = {
                            "name": message["@name"],
                            "tags_name": tags_name,
                            "tags_id": tags_id,
                        }
                        protocol_msgs_admin_name[message["@name"]] = {
                            "msgtype": message["@msgtype"],
                            "tags_name": tags_name,
                            "tags_id": tags_id,
                        }
                    elif message["@msgcat"] == "app":
                        protocol_msgs_app_msgtype[message["@msgtype"]] = {
                            "name": message["@name"],
                            "tags_name": tags_name,
                            "tags_id": tags_id,
                        }
                        protocol_msgs_app_name[message["@name"]] = {
                            "msgtype": message["@msgtype"],
                            "tags_name": tags_name,
                            "tags_id": tags_id,
                        }
                self._protocol_msgs['admin'].update(protocol_msgs_admin_msgtype)
                self._protocol_msgs['admin'].update(protocol_msgs_admin_name)
                self._protocol_msgs['app'].update(protocol_msgs_app_msgtype)
                self._protocol_msgs['app'].update(protocol_msgs_app_name)

                protocol_header_name = {'name': {}}
                protocol_header_id = {'n_id': {}}
                protocol_trailer_name = {'name': {}}
                protocol_trailer_id = {'n_id': {}}
                for field in protocol_dict["header"]["field"]:
                    protocol_header_name['name'][field["@name"]] = {'required': field["@required"]}
                    protocol_header_id['n_id'][self._protocol_tags[field["@name"]]['n_id']] = {'required': field["@required"]}

                for field in protocol_dict["trailer"]["field"]:
                    protocol_trailer_name['name'][field["@name"]] = {'required': field["@required"]}
                    protocol_trailer_id['n_id'][self._protocol_tags[field["@name"]]['n_id']] = {'required': field["@required"]}

                self._protocol_header.update(protocol_header_name)
                self._protocol_header.update(protocol_header_id)
                self._protocol_trailer.update(protocol_trailer_name)
                self._protocol_trailer.update(protocol_trailer_id)
                

        except FileNotFoundError:
            print(
                "Error: FIX Version {} is either not supported by this library, or could not be found.".format(
                    self.version
                )
            )

    def _recurse_groups(self, group, use_id):
        group_dict = {}
        if use_id:
            group_key = self._protocol_tags[group["@name"]]['n_id']
        else:
            group_key = group["@name"] 
        group_dict[group_key] = {
            "required": group["@required"],
            "members": OrderedDict(),
        }
        if isinstance(group["field"], list):
            for tag in group["field"]:
                if use_id:
                    group_dict[group_key]["members"][self._protocol_tags[tag["@name"]]['n_id']] = {'required': tag["@required"]}
                else:
                    group_dict[group_key]["members"][tag["@name"]] = {'required': tag["@required"]}
        else:
            if use_id:
                group_dict[group_key]["members"][self._protocol_tags[group["field"]["@name"]]['n_id']] = {'required': group["field"]["@required"]}
            else:
                group_dict[group_key]["members"][group["field"]["@name"]] = {'required': group["field"]["@required"]}                
        try:
            if isinstance(group["group"], list):
                for subgroup in group["group"]:
                    group_dict[group_key]["members"].update(
                        self._recurse_groups(subgroup, use_id)
                    )
            else:
                group_dict[group_key]["members"].update(
                    self._recurse_groups(group["group"], use_id)
                )
        except KeyError:
            pass
        return group_dict


# message = interface.gen_message(type)
# message.add_tag(tag)
# message.add_tags(tag, tag, tag)

# message.commit
# push(message)
# message = callback()
# data = message.fields
