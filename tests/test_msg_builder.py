from coinbasepro_fix_client.context import *
from coinbasepro_fix_client.message_builder import *


if __name__ == "__main__":
    context = FIXContext("4.2_coinbase")
    msg_builder = FIXMessageBuilder(context)

    message = msg_builder.generate_message("A")
    msg_builder.add_tag(98, 0, message)
    msg_builder.add_tag(108, 15, message)
    msg_builder.add_tag(49, "fhdsfhdsufysir3uryoewiryw", message.header)
    msg_builder.add_tag(56, "Coinbase", message.header)
    msg_builder.add_tag(554, "g9392gg37rgh2feg", message)
    msg_builder.add_tag(8013, "S", message)
    msg_builder.add_tag(9406, "N", message)
    msg_builder.add_tag(96, "JDFHKJSDFHJKSDFHUWHFHFU", message)

    test_response=b'8=FIX.4.2\x019=173\x0135=A\x0152=20190911-15:12:30.288\x0149=Coinbase\x0156=6d92bab74cbf93a8ed3712227221ef68\x0134=1\x0196=hojYh+uilGgOrcqznjrEsl7b9k1GcxuuPvE6A+hGs6w=\x0198=0\x01108=15\x01554=l2acppd0w59\x018013=S\x019406=N\x0110=244\x01'
    messages, test_response = msg_builder.parse(test_response)
    print([print(x) for x in messages], test_response)

    
