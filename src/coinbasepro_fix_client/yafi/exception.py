#!/usr/bin/env python


class Error(Exception):
    def __init__(self, message):
        self.message = message


class Invalid(Exception):
    def __init__(self, message):
        self.message = message


class MessageNotReady(Invalid):
    pass


class Invalidtag(Invalid):
    pass


class ReadOnlyError(Error):
    pass
