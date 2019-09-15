#!/usr/bin/env python
from pathlib import Path
import xmltodict

import os

_ROOT = os.path.abspath(os.path.dirname(__file__))

class FIXContext(object):
    def __init__(self, protocol_descriptor, input_mode='quickfix'): #TODO split all dictionaries into id/name keys, then merge them into main dict for each.
        self.version = None
        self._protocol_tags_name = {}   # name: (num, type)
        self._protocol_tags_num = {}    # num: (name, type)
        self._protocol_msgs_admin = {}  # msgtype: (name, {num: req})
        self._protocol_msgs_app = {}    # msgtype: (name, {num: req})
        self._protocol_header = {}      # num: req
        self._protocol_trailer = {}     # num: req
        self.initialized = False
        impl_funcs = {'quickfix': self._quickfix_xml_loader}

        try:
            impl_funcs[input_mode](protocol_descriptor)
        except KeyError:
            print(f"Error: {input_mode} has not been implemented yet. Existing implementations: {[i for i in impl_funcs.keys()]}")

    def _quickfix_xml_loader(self, protocol_descriptor):
        protocol_file = Path(os.path.join(_ROOT, "fix_protocol", protocol_descriptor + ".xml"))
        try:
            with protocol_file.open() as file:
                protocol_dict = xmltodict.parse(file.read())['fix']
                # Extract version
                self.version = protocol_dict['@type'] + '.' + protocol_dict['@major'] + "." + protocol_dict['@minor']


                for tag in protocol_dict['fields']['field']:
                    self._protocol_tags_name[tag['@name']] = (int(tag['@number']), tag['@type'])
                    self._protocol_tags_num[int(tag['@number'])] = (tag['@name'], tag['@type'])

                for message in protocol_dict['messages']['message']:
                    tags = {}
                    if isinstance(message['field'], list):
                        for tag in message['field']:
                            tags[tag['@name']] = tag['@required']
                    else:
                        tags[message['field']['@name']] = message['field']['@required']
                    try:
                        if isinstance(message['group'], list):
                            for group in message['group']:
                                group_dict = self._recurse_groups(group)
                                tags.update(group_dict)
                        else:
                                group_dict = self._recurse_groups(message['group'])
                                tags.update(group_dict)
                    except KeyError:
                        pass
                    if message['@msgcat'] == 'admin':
                        self._protocol_msgs_admin[message['@msgtype']] = (message['@name'], tags)
                    elif message['@msgcat'] == 'app':
                        self._protocol_msgs_app[message['@msgtype']] = (message['@name'], tags)
                
                for field in protocol_dict['header']['field']:
                    self._protocol_header[field['@name']] = field['@required']

                for field in protocol_dict['trailer']['field']:
                    self._protocol_trailer[field['@name']] = field['@required']


        except FileNotFoundError:
            print(
                "Error: FIX Version {} is either not supported by this library, or could not be found.".format(
                    self.version
                )
            )

    def _recurse_groups(self, group):
        group_dict = {}
        group_dict[group['@name']] = [group['@required'], {}]
        if isinstance(group['field'], list):
            for tag in group['field']:
                group_dict[group['@name']][1][tag['@name']] = tag['@required']
        else:
            group_dict[group['@name']][1][group['field']['@name']] = group['field']['@required']
        try:
            if isinstance(group['group'], list):
                for subgroup in group['group']:
                    group_dict[group['@name']][1].update(self._recurse_groups(subgroup))
            else:
                group_dict[group['@name']][1].update(self._recurse_groups(group['group']))
        except KeyError:
            pass
        group_dict[group['@name']] = tuple(group_dict[group['@name']])
        return group_dict


#message = interface.gen_message(type)
#message.add_tag(tag)
#message.add_tags(tag, tag, tag)

#message.commit
#push(message)
#message = callback()
#data = message.fields
