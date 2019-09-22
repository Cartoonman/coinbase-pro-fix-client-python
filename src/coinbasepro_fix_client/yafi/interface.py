#!/usr/bin/env python
from collections import defaultdict, OrderedDict, Callable
import time, datetime


class DefaultOrderedDict(OrderedDict):
    # Source: http://stackoverflow.com/a/6190500/562769
    def __init__(self, default_factory=None, *a, **kw):
        if (default_factory is not None and not isinstance(default_factory, Callable)):
            raise TypeError('first argument must be callable')
        OrderedDict.__init__(self, *a, **kw)
        self.default_factory = default_factory

    def __getitem__(self, key):
        try:
            return OrderedDict.__getitem__(self, key)
        except KeyError:
            return self.__missing__(key)

    def __missing__(self, key):
        if self.default_factory is None:
            raise KeyError(key)
        self[key] = value = self.default_factory()
        return value

    def __reduce__(self):
        if self.default_factory is None:
            args = tuple()
        else:
            args = self.default_factory,
        return type(self), args, None, None, self.items()

    def copy(self):
        return self.__copy__()

    def __copy__(self):
        return type(self)(self.default_factory, self)

    def __deepcopy__(self, memo):
        import copy
        return type(self)(self.default_factory,
                          copy.deepcopy(self.items()))

    def __repr__(self):
        return 'OrderedDefaultDict(%s, %s)' % (self.default_factory,
                                               OrderedDict.__repr__(self))



class Tag(object):
    def __init__(
        self,
        FIX_id=None,
        FIX_name=None,
        FIX_type=None,
        FIX_data=None,
    ):
        self.data = FIX_data
        self.id_ = FIX_id
        self.name = FIX_name
        self.type = FIX_type

    def set_data(self, x):
        # self.check(x) TODO
        self.data = x

    def get_data(self):
        return self.data

    def gen_fix(self):
        return str(self.id_) + "=" + str(self.data) + "\x01"

    def length(self):
        return len(self.gen_fix())

    def is_none(self):
        return self.data == None

    def __repr__(self):
        return "Tag [{}({}): {}]".format(self.name, self.id_, self.data)

    
class Group(object):
    def __init__(self, id_, name, group_def, tag_context):
        self.group_def = group_def
        self.data = DefaultOrderedDict(dict)
        self.tag_dict = tag_context
        self.id_ = id_
        self.name = name
        self.subgroups = {}
        for k, v in self.group_def.items():
            if 'members' in v:
                self.subgroups[k] = Group(k, self.tag_dict[k]['name'], v['members'], tag_context)
                self.data[k]['members'] = []

    def add_subgroup(self, group):
        if group.id_ in self.subgroups and group.id_ in self.group_def:
            self.data[group.id_]['members'].append(group)


    def __getitem__(self, key):
        if isinstance(key, str):
            try:
                key = int(key)
            except ValueError:
                key = self.tag_dict[key]['n_id']
        if key in self.group_def:
            return data[key]['data']
        elif key in self.tag_dict:
            pass
            # Exception key not in dict (keyerror)
        else:
            pass
            # Exception key invalid

    def __setitem__(self, key, value):
        if isinstance(key, str):
            try:
                key = int(key)
            except ValueError:
                key = self.tag_dict[key]['n_id'] 

        if key in group_def:
            self.data[key]['data'] = value
            return
        else:
            # TODO issue warning
            return






msg = gen_message(fdsfds)
group = msg.get_group_template('NoOrders')
group.add_tag....
group.get_subgroups()

message.get_group_template()
# fill group 
group.member.set()
group.get_subgroup_template()
group.set_subgroup()
message.add_group(group)
message.add_group(group)
group{
    defmine members

}


tags :
    452: data: Tag()
    623: data: Tag() 
         member: [
        {
            5: Tag()
            645: Tag()
            78: Tag() [
                {
                    85: Tag()
                    6456: Tag()
                },
                {
                    85: Tag()
                    6456: Tag()
                },
            ]
        },
        {
            5: Tag()
            645: Tag()
            78: Tag() [
                {
                    85: Tag()
                    6456: Tag()
                },
                {
                    85: Tag()
                    6456: Tag()
                },
            ]
        },
    ]

class Message(object):
    def __init__(self, header, trailer, req_tags, opt_tags, groups, msg_cat, id_, name, tag_context):
        self.header_def = header
        self.required_tags_def = req_tags
        self.optional_tags_def = opt_tags
        self.trailer_def = trailer
        self.groups = groups
        self.id_ = id_
        self.name = name
        self.data = {'header': DefaultOrderedDict(dict), 'tags': DefaultOrderedDict(dict), 'trailer': DefaultOrderedDict(dict)}
        self.fix_msg_payload = None
        self.tag_dict = tag_context

    def __getitem__(self, key):
        if isinstance(key, str):
            try:
                key = int(key)
            except ValueError:
                key = self.tag_dict[key]['n_id']
        merged_data = {**self.data['header'], **self.data['tags'], **self.data['trailer']}
        if key in merged_data:
            return merged_data[key]['data']
        elif key in self.tag_dict:
            pass
            # Exception key not in dict (keyerror)
        else:
            pass
            # Exception key invalid

    def __setitem__(self, key, value):
        if isinstance(key, str):
            try:
                key = int(key)
            except ValueError:
                key = self.tag_dict[key]['n_id']

        if key in self.header_def:
            self.data['header'][key]['data'] = value
            return
        elif key in self.trailer_def:
            self.data['trailer'][key]['data'] = value
            return         
        elif key in {**self.required_tags_def, **self.optional_tags_def}:
            self.data['tags'][key]['data'] = value
            return
        else:
            # TODO issue warning
            return

    def add_group(self, group):
        if group.id_ in self.groups and group.id_ in {**self.req_tags, **self.opt_tags}:
            if not isinstance(self.data['tags'][group.id_]['members'], list):
                self.data['tags'][group.id_]['members'] = []
            self.data['tags'][group.id_]['members'].append(group)


    def get_group_template(self, tag_id):
        if key in self.groups:
            return self.data[key]
        else:
            raise KeyError

    def ready_pkg(self):
        temp_set = self.required_tags
        for tag in self.tags.keys():
            temp_set.discard(tag)
        if len(temp_set) != 0:
            return False, temp_set
        else:
            return True, None

    def ready_send(self):
        temp_set_header = self.header.required_tags
        for tag in self.header.tags.keys():
            temp_set_header.discard(tag)
        if len(temp_set_header) != 0:
            return False, "HEAD", temp_set_header

        pkg_state, msg_set = self.ready_pkg()
        if not pkg_state:
            return False, "MSG", msg_set

        temp_set_trailer = self.trailer.required_tags
        for tag in self.trailer.tags.keys():
            temp_set_trailer.discard(tag)
        if len(temp_set_trailer) != 0:
            return False, "TAIL", temp_set_trailer

        return True, None, None

    def __str__(self):
        print_msg = "Message [{}({})]: \n[\n\t".format(self.name, self.id_)
        print_msg += "HEADER: \n\t"
        for tags in self.data['header'].tags.values():
            for tag in tags:
                print_msg += repr(tag)
                print_msg += "\n\t"
        print_msg += "MSG: \n\t"
        for tags in self.tags.values():
            for tag in tags:
                print_msg += repr(tag)
                print_msg += "\n\t"
        print_msg += "TRAILER: \n\t"
        for tags in self.trailer.tags.values():
            for tag in tags:
                print_msg += repr(tag)
                print_msg += "\n\t"
        print_msg += "\n]\n"

        return print_msg


class FIXInterface(object):
    def __init__(self, context):
        self._context = context

    def generate_message(self, id_in):
        # Check if name is in context
        if id_in in self._context._protocol_msgs['admin']:
            msg_cat = 'admin'
        elif id_in in self._context._protocol_msgs['app']:
            msg_cat = 'app'
        else:
            pass
            # throw exception message invalid

        # Check if we have name or ID
        msg_def = self._context._protocol_msgs[msg_cat]
        if 'name' in msg_def:
            msg_type = id_in
            name = msg_def['name']
        elif 'msgtype' in msg_def:
            msg_type = msg_def['msgtype']
            name = id_in
        else:
            pass
            # throw unknown exception

        # Set Header and Trailer Defs
        header = self._context._protocol_header
        trailer = self._context._protocol_trailer

        # Set req and opt tags
        req_tags = {'tags_name': {}, 'tags_id': {}}
        opt_tags = {'tags_name': {}, 'tags_id': {}}

        req_tags['tags_name'].update(dict(filter(lambda x: x[1]['required'] == 'Y', self._context._protocol_msgs[msg_cat][msg_type]['tags_name'].items())))
        req_tags['tags_id'].update(dict(filter(lambda x: x[1]['required'] == 'Y', self._context._protocol_msgs[msg_cat][msg_type]['tags_id'].items())))
        opt_tags['tags_name'].update(dict(filter(lambda x: x[1]['required'] == 'N', self._context._protocol_msgs[msg_cat][msg_type]['tags_name'].items())))
        opt_tags['tags_id'].update(dict(filter(lambda x: x[1]['required'] == 'N', self._context._protocol_msgs[msg_cat][msg_type]['tags_id'].items())))

        merged_tags_id = {**req_tags['tags_id'], **opt_tags['tags_id']}
        groups = {}
        for k, v in merged_tags_id.items():
            if 'members' in v:
                groups[k] = v['members']

        return Message(
            header=header, trailer=trailer, req_tags=req_tags, opt_tags=opt_tags, groups=groups id_=msg_type, name=name, msg_cat=msg_cat, tag_context=self._context._protocol_tags
        )

    def load_message(self, msg_array):
        message_type = msg_array[2].split("=", 1)[1]
        message = self.generate_message(message_type)

        # Passes
        for tag in msg_array:
            id_ = tag.split("=", 1)[0]
            data = tag.split("=", 1)[1]
            if id_ in message.header.required_tags.union(message.header.optional_tags):
                self.add_tag(id_, data, message.header)
            if id_ in message.required_tags.union(message.optional_tags):
                self.add_tag(id_, data, message)
            if id_ in message.trailer.required_tags.union(
                message.trailer.optional_tags
            ):
                self.add_tag(id_, data, message.trailer)

        # Check for orphaned tags.
        total_msg_tags = set(
            list(message.header.tags.keys())
            + list(message.tags.keys())
            + list(message.trailer.tags.keys())
        )
        for tag in msg_array:
            if tag.split("=", 1)[0] not in total_msg_tags:
                print(
                    "WARN: {} NOT IN MESSAGE DEFINITION. MESSAGE: {}".format(
                        tag, message
                    )
                )

        return message

    def _prepare_ctrl_msg(self, ctrl_msg_def, msg_id):
        required_tags = ctrl_msg_def["required"]
        optional_tags = ctrl_msg_def["optional"]
        # Fix for potentially blank entries.
        if len(required_tags) == 1 and required_tags[0] == "":
            required_tags = set([])
        if len(optional_tags) == 1 and optional_tags[0] == "":
            optional_tags = set([])
        return self.Message(
            None,
            None,
            set(required_tags),
            set(optional_tags),
            ctrl_msg_def["id"],
            ctrl_msg_def["name"],
            ctrl_msg_def["description"],
        )

    def _prepare_msg(self, msg_def, header, trailer):
        required_tags = msg_def["required"]
        optional_tags = msg_def["optional"]
        # Fix for potentially blank entries.
        if len(required_tags) == 1 and required_tags[0] == "":
            required_tags = set([])
        if len(optional_tags) == 1 and optional_tags[0] == "":
            optional_tags = set([])
        return self.Message(
            header,
            trailer,
            set(required_tags),
            set(optional_tags),
            msg_def["id"],
            msg_def["name"],
            msg_def["description"],
        )

    def decode(self, id_):
        return self.FIX_Context._protocol_tags[id_]["FIX_name"]

    def _gen_tag(self, id_, data=None):
        tag_struct = self.FIX_Context._protocol_tags[id_]
        return self.Tag(
            tag_struct["FIX_id"],
            tag_struct["FIX_name"],
            tag_struct["FIX_type"],
            tag_struct["FIX_description"],
            data,
        )

    def add_tag(self, id_, data, message):
        if str(id_) in message.required_tags.union(message.optional_tags):
            if not message._primed:
                message.tags[str(id_)].append(self._gen_tag(str(id_), data))
            else:
                raise ReadOnlyError("Message cannot be modified after primed state.")
        else:
            raise InvalidTag(f"Tag {str(id_)} is not in Message {message.name}")

    def prepare(self, message, seq, timestamp):
        # Check if ready (all req tags set in msg)
        if message.ready_pkg()[0] is not True:
            raise MessageNotReady("Message does not pass ready_pkg() check.")

        # Fill in rest of Header
        self.add_tag(34, seq, message.header)
        self.add_tag(52, timestamp, message.header)
        self.add_tag(35, message.id_, message.header)

        body_length = self.calc_body_length(message)

        self.add_tag(9, body_length, message.header)
        self.add_tag(8, "FIX.4.2", message.header)

        # Calculate CheckSum
        checksum = self.gen_checksum(message)
        self.add_tag(10, checksum, message.trailer)

        # Generate Raw Fix Msg
        fix_msg = self.gen_raw_fix(message)
        message.fix_msg_payload = fix_msg
        message._primed = True

    def calc_body_length(self, message):
        # Calculate body length
        body_length = sum(
            [
                tag.length()
                for taglist in [
                    message.header.tags.values(),
                    message.tags.values(),
                    message.trailer.tags.values(),
                ]
                for tags in taglist
                for tag in tags
            ]
        )
        # Remove length of tags 8, 9, and 10 if already in message.
        for id_ in ["8", "9"]:
            if id_ in message.header.tags.keys():
                body_length -= message.header.tags[id_][0].length()
        if "10" in message.trailer.tags.keys():
            body_length -= message.trailer.tags["10"][0].length()
        return body_length

    def gen_checksum(self, message):
        checksum = sum(
            [
                ord(char)
                for taglist in [
                    message.header.tags.values(),
                    message.tags.values(),
                    message.trailer.tags.values(),
                ]
                for tags in taglist
                for tag in tags
                for char in tag.gen_fix()
            ]
        )
        # Take out the checksum itself if it exists already.
        if "10" in message.trailer.tags.keys():
            checksum -= sum(
                [ord(char) for char in message.trailer.tags["10"][0].gen_fix()]
            )
        checksum = str(checksum % 256)
        while len(checksum) < 3:
            checksum = "0" + checksum
        return checksum

    def gen_raw_fix(self, message):
        # Generate first 3 tags
        fix_msg = (
            message.header.tags["8"][0].gen_fix()
            + message.header.tags["9"][0].gen_fix()
            + message.header.tags["35"][0].gen_fix()
        )

        # Generate Header
        for k, v in message.header.tags.items():
            if k in ["8", "9", "35"]:
                continue
            for tag in v:
                fix_msg += tag.gen_fix()

        # Generate Msg
        fix_msg += "".join(
            [tag.gen_fix() for tags in message.tags.values() for tag in tags]
        )

        # Generate Trailer
        for k, v in message.trailer.tags.items():
            if k in ["10"]:
                continue
            for tag in v:
                fix_msg += tag.gen_fix()
        # Generate Checksum
        fix_msg += message.trailer.tags["10"][0].gen_fix()
        fix_msg = fix_msg.encode("ascii")
        return fix_msg

    def validate(self, message):
        calculated_checksum = self.gen_checksum(message)
        calculated_body_length = self.calc_body_length(message)
        if calculated_checksum != message.trailer.tags["10"][0].get():
            print(
                f"CHECKSUM MISMATCH: CALCULATED {calculated_checksum}, GIVEN {message.trailer.tags['10'][0].get()}"
            )
            return False
        if calculated_body_length != int(message.header.tags["9"][0].get()):
            print(
                (
                    f"BODY LENGTH MISMATCH: CALCULATED {calculated_body_length}, GIVEN {message.header.tags['9'][0].get()}"
                )
            )
            return False
        return True

    def parse(self, input_response):
        working_data = input_response.decode("ascii")
        pointers_front = []
        pointers_rear = []
        del_rear = None
        messages = []
        working_data_arr = [x for x in working_data.split("\x01")]
        for tag in range(len(working_data_arr)):
            if working_data_arr[tag][:2] == "8=":  # Needs to be another check here
                pointers_front.append(tag)
            if working_data_arr[tag][:3] == "10=" and len(working_data_arr[tag]) == 6:
                pointers_rear.append(tag)

        for front, rear in zip(pointers_front, pointers_rear):
            message = self.load_message(working_data_arr[front : rear + 1])
            # Check checksum and message length matches for security.
            del_rear = rear
            if not (self.validate(message)):
                continue
            else:
                messages.append(message)

        # Clear buffer if all we got was junk with no front tag found.
        if len(working_data_arr) != 0 and len(pointers_front) == 0 and del_rear is None:
            working_data_arr = []

        # Remove the corresponding data from original working data. (reconstruct remaining?)
        if del_rear is not None:
            working_data_arr = working_data_arr[del_rear + 1 :]
        # Take care of trailing control char case.
        if len(working_data_arr) == 1 and working_data_arr[0] == "":
            working_data_arr = []

        remainder_buffer = ""
        # Reconstruct the remainder in buffer
        if len(working_data_arr) != 0:
            remainder_buffer = "\x01".join(working_data_arr)
        remainder_buffer = remainder_buffer.encode("ascii")

        # Return results
        # (messages_array, remaining string buffer)
        return messages, remainder_buffer

    def _get_req_tags(self, id_):
        return {
            i: self._gen_tag(i)
            for i in self.FIX_Context._protocol_msgs[id_]["required"]
        }

    def _get_opt_tags(self, id_, opt_tag_list):
        return {
            i: self._gen_tag(i)
            for i in [
                str(tag)
                for tag in opt_tag_list
                if str(tag) in self.FIX_Context._protocol_msgs[id_]["optional"]
            ]
        }
