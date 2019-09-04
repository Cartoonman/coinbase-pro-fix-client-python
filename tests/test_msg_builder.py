from fix_context import *
from fix_msg_builder import *


if __name__ == "__main__":
    context = FIXContext("4.2")
    msg_builder = FIXMessageBuilder(context)

    msg_builder.gen_msg("k")
