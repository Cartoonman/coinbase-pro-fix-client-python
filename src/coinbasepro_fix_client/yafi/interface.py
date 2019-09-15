#!/usr/bin/env python
from collections import defaultdict
import time, datetime

class Tag(object):
    def __init__(
        self,
        FIX_id=None,
        FIX_name=None,
        FIX_type=None,
        FIX_description=None,
        FIX_data=None,
    ):
        self.data = FIX_data
        self.id_ = FIX_id
        self.name = FIX_name
        self.type = FIX_type
        self.desc = FIX_description

    def set(self, x):
        self.check(x)
        self.data = repr(x)

    def get(self):
        return self.data

    def gen_fix(self):
        return str(self.id_) + "=" + str(self.data) + "\x01"

    def length(self):
        return len(self.gen_fix())

    def is_none(self):
        return self.data == None

    def __repr__(self):
        return "Tag [{}({}): {}]".format(self.name, self.id_, self.data)


class Message(object):
    def __init__(self, header, trailer, req_tags, opt_tags, groups, id_, name, context):
        self.header = header
        self.required_tags = req_tags
        self.optional_tags = opt_tags
        self.groups = groups
        self.trailer = trailer
        self.id_ = id_
        self.name = name
        self.data = {}
        self.fix_msg_payload = None
        self._primed = False

    def __getitem__(self, key):
        if key in self.data:
            return self.data[key]
        else:
            raise KeyError

    def __setitem__(self, key, value):
        self.data[key] = str(value)

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
        for tags in self.header.tags.values():
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

        # def __init__(self, header, trailer, req_tags, opt_tags, groups, id_, name, context):



        # Get name
        if id_in in self._context._protocol_msgs_admin:
            id_ = id_in
            name = self._context._protocol_msgs_admin
        name = self.FIX_Context._protocol_


        return Message(header, trailer, req_tags, opt_tags, groups, id_, name, self._context)

        header_def = self.FIX_Context._protocol_msgs["HEAD"]
        trailer_def = self.FIX_Context._protocol_msgs["TAIL"]
        msg_def = self.FIX_Context._protocol_msgs[id_in]

        header = self._prepare_ctrl_msg(header_def, id_in)
        trailer = self._prepare_ctrl_msg(trailer_def, None)
        msg = self._prepare_msg(msg_def, header, trailer)
        return msg

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
