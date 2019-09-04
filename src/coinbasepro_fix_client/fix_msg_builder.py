#!/usr/bin/env python
class FIXMessageBuilder(object):
    def __init__(self, FIX_Context):
        self.FIX_Context = FIX_Context

    class Tag(object):
        def __init__(
            self, FIX_id=None, FIX_name=None, FIX_type=None, FIX_description=None
        ):
            self.data = None
            self.id_ = FIX_id
            self.name = FIX_name
            self.type = FIX_type
            self.desc = FIX_description

        def set(self, x):
            self.check(x)
            self.data = repr(x)

        def get(self):
            return self.data

        def is_none(self):
            return self.data == None

        def __repr__(self):
            return "Tag[{}:{} | data: {}]".format(self.id_, self.name, self.data)

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
        def __init__(self, header, req_tags, opt_tags, trailer, id_, name, desc):
            self.header = header
            self.required_tags = req_tags
            self.optional_tags = opt_tags
            self.trailer = trailer
            self.id_ = id_
            self.name = name
            self.desc = desc

        def ready(self):
            for _, tag in self.required_tags.items():
                if tag.is_none:
                    return False
            return True

    class ControlStruct(object):
        def __init__(self, req_tags, opt_tags=[]):
            self.required_tags = req_tags
            self.optional_tags = opt_tags

    def generate_message(
        self, id_in, opt_tag_list=[], header_opt_tag_list=[], trailer_opt_tag_list=[]
    ):
        header = self._gen_control_msg("HEAD", header_opt_tag_list)
        req_tags = self._get_req_tags(id_in)
        opt_tags = self._get_opt_tags(id_in, opt_tag_list)
        trailer = self._gen_control_msg("TAIL", trailer_opt_tag_list)
        id_ = self.FIX_Context._protocol_msgs[id_in]["id"]
        name = self.FIX_Context._protocol_msgs[id_in]["name"]
        desc = self.FIX_Context._protocol_msgs[id_in]["description"]

        message = self.Message(header, req_tags, opt_tags, trailer, id_, name, desc)
        return message

    def decode(self, id_):
        return self.FIX_Context._protocol_tags[id_]["FIX_name"]

    def _gen_tag(self, id_):
        tag_struct = self.FIX_Context._protocol_tags[id_]
        return self.Tag(
            tag_struct["FIX_id"],
            tag_struct["FIX_name"],
            tag_struct["FIX_type"],
            tag_struct["FIX_description"],
        )

    def _gen_control_msg(self, struct_type, opt_tag_list=[]):
        req_tags = self._get_req_tags(struct_type)
        opt_tags = self._get_opt_tags(struct_type, opt_tag_list)
        control_message = self.ControlStruct(req_tags, opt_tags)
        return control_message

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
