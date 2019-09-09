#!/usr/bin/env python
from pathlib import Path
import csv

import os

_ROOT = os.path.abspath(os.path.dirname(__file__))


def _get_fix_protocol(version, file_name):
    return os.path.join(_ROOT, "fix_protocol", version, file_name)


class FIXContext(object):
    def __init__(self, version):
        self.version = version
        self._protocol_tags = {}
        self._protocol_msgs = {}
        self.initialized = False

        messages_file = Path(_get_fix_protocol(version, "messages.csv"))
        tags_file = Path(_get_fix_protocol(version, "tags.csv"))
        try:
            with tags_file.open() as file:
                reader = csv.DictReader(file)
                for row in reader:
                    self._protocol_tags[row["FIX_id"]] = row

            with messages_file.open() as file:
                reader = csv.DictReader(file)
                for row in reader:
                    self._protocol_msgs[row["id"]] = row
                    self._protocol_msgs[row["id"]]["required"] = self._protocol_msgs[
                        row["id"]
                    ]["required"].split(";")
                    self._protocol_msgs[row["id"]]["optional"] = self._protocol_msgs[
                        row["id"]
                    ]["optional"].split(";")
            self.initialized = True
        except FileNotFoundError:
            print(
                "Error: FIX Version {} is either not supported by this library, or could not be found.".format(
                    self.version
                )
            )
