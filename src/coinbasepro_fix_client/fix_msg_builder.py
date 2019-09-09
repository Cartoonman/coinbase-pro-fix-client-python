#!/usr/bin/env python
from collections import defaultdict
import time, datetime


class FIXMessageBuilder(object):
    def __init__(self, FIX_Context):
        self.FIX_Context = FIX_Context

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

        def __str__(self):
            return """-----------   \
                \nTag:  {} {} ,        \
                \nType: {} ,        \
                \nData: {} ,        \
                \nDescription: {}   \
                """.format(
                self.id_, self.name, self.type, self.data, self.desc
            )

    class Message(object):
        def __init__(self, header, trailer, req_tags, opt_tags, id_, name, desc):
            self.header = header
            self.required_tags = req_tags
            self.optional_tags = opt_tags
            self.trailer = trailer
            self.id_ = id_
            self.name = name
            self.desc = desc
            self.tags = defaultdict(list)
            self.fix_msg_payload = None

        def ready_pkg(self):
            temp_set = self.required_tags
            for tag in self.tags.keys():
                temp_set.discard(tag)
            if len(temp_set) != 0:
                return False, temp_set
            else:
                return True, None

        def gen_fix_one(self, id_):
            return

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

    def generate_message(self, id_in):
        header_def = self.FIX_Context._protocol_msgs["HEAD"]
        trailer_def = self.FIX_Context._protocol_msgs["TAIL"]
        msg_def = self.FIX_Context._protocol_msgs[id_in]

        header = self._prepare_ctrl_msg(header_def, id_in)
        trailer = self._prepare_ctrl_msg(trailer_def, None)
        msg = self._prepare_msg(msg_def, header, trailer)
        return msg

    def _prepare_ctrl_msg(self, ctrl_msg_def, msg_id):
        return self.Message(
            None,
            None,
            set(ctrl_msg_def["required"]),
            set(ctrl_msg_def["optional"]),
            ctrl_msg_def["id"],
            ctrl_msg_def["name"],
            ctrl_msg_def["description"],
        )

    def _prepare_msg(self, msg_def, header, trailer):
        return self.Message(
            header,
            trailer,
            set(msg_def["required"]),
            set(msg_def["optional"]),
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
            message.tags[str(id_)].append(self._gen_tag(str(id_), data))
            return True  # Todo change this to exception
        else:
            return False

    def prepare(self, message, seq):
        # Check if ready (all req tags set in msg)
        if message.ready_pkg()[0] is not True:
            return False

        # Fill in rest of Header
        self.add_tag(34, seq, message.header)
        self.add_tag(
            52,
            str(datetime.datetime.utcnow()).replace("-", "").replace(" ", "-")[:-3],
            message.header,
        )
        self.add_tag(35, message.id_, message.header)

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
        self.add_tag(9, body_length, message.header)
        self.add_tag(8, "FIX.4.2", message.header)

        # Calculate CheckSum
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
        print(checksum)
        checksum = str(checksum % 256)
        while len(checksum) < 3:
            checksum = "0" + checksum
        self.add_tag(10, checksum, message.trailer)

        # Save full fix msg into msg field.
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
        message.fix_msg_payload = fix_msg
        return True

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
