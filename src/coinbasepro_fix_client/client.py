#!/usr/bin/env python

import threading

# Adapted from great example:
# https://stackoverflow.com/questions/47110454/how-to-send-fix-logon-message-with-python-to-gdax/

class CoinbaseFIXClient(object):
    def __init__(self, passphrase, key, secret, fix_protocol_version="4.2_coinbase", host="fix.pro.coinbase.com", port=4198):
        self._context = FIXContext(fix_protocol_version)
        self._builder = FIXMessageBuilder(_context)
        if None in [passphrase, key, secret]:
            print("Error: one or more credentials were missing")
        self.host = host
        self.port = port
        self.msg_seq_counter = 1

        _initialize_connection()
        _login(passphrase, key, secret)


    def _initialize_connection(self):
        self.ssl_context = ssl.create_default_context()
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.ssl_sock = ssl_context.wrap_socket(s, server_hostname=self.host)
        ssl_sock.connect((self.host, self.port))
        threading.Thread(target=rx_thread, daemon=True)


    def _login(self, passphrase, key, secret):
        message = msg_builder.generate_message("A")
        timestamp = str(datetime.datetime.utcnow()).replace("-", "").replace(" ", "-")[:-3]
        msg_builder.add_tag(98, 0, message)
        msg_builder.add_tag(108, 15, message)
        msg_builder.add_tag(49, key, message.header)
        msg_builder.add_tag(56, "Coinbase", message.header)
        msg_builder.add_tag(554, passphrase, message)
        msg_builder.add_tag(8013, "S", message)
        msg_builder.add_tag(9406, "N", message)
        msg_builder.add_tag(96, self._hmac_sign(timestamp, "A", self.msg_seq_counter, key, passphrase, secret), message)
        msg_builder.prepare(message, self.msg_seq_counter, timestamp)


    def _hmac_sign(self, t, msg_type, seq_num, api_key, password, secret):
        message = "\x01".join([t, msg_type, seq_num, api_key, "Coinbase", password]).encode(
            "utf-8"
        )
        hmac_key = base64.b64decode(secret)
        signature = hmac.new(hmac_key, message, hashlib.sha256)
        return base64.b64encode(signature.digest()).decode()

    def rx_thread(self):
        response = ""
        while(True):
            response += ssl_sock.recv(4096)
            # Pull out msgs
            # Place on msg dict (of queues) with lock keeping accesses correct between threads. 

