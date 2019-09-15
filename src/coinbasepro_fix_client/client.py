#!/usr/bin/env python

import threading
from collections import defaultdict
import ssl
import socket
import hmac
import hashlib
import base64
import datetime
from queue import Queue, Empty
from yafi import FIXContext, FIXInterface

# Adapted from great example:
# https://stackoverflow.com/questions/47110454/how-to-send-fix-logon-message-with-python-to-gdax/


class ThreadSafeDefaultDict(defaultdict):
    def __init__(self, *p_arg, **n_arg):
        defaultdict.__init__(self, *p_arg, **n_arg)
        self._lock = threading.Lock()

    def __enter__(self):
        self._lock.acquire()
        return self

    def __exit__(self, type, value, traceback):
        self._lock.release()


class CoinbaseFIXClient(object):
    def __init__(
        self,
        passphrase,
        key,
        secret,
        fix_protocol_version="4.2",
        host="fix.pro.coinbase.com",
        port=4198,
    ):
        self._context = FIXContext(fix_protocol_version)
        self._interface = FIXInterface(self._context)
        if None in [passphrase, key, secret]:
            print("Error: one or more credentials were missing")
        self.host = host
        self.port = port
        self.tx_seq_counter = 1
        self.rx_seq_counter = 1
        self.rx_buffer = ThreadSafeDefaultDict(lambda:Queue(10))
        self.tx_buffer = Queue(10)
        self.shutdown_lock = threading.Lock()
        self.tx_counter_lock = threading.Lock()
        self.rx_counter_lock = threading.Lock()
        self.shutdown = False
        self.api_key = key
        self.passphrase = passphrase
        self.secret = secret
        self.connected = False

    def _initialize_connection(self):
        self.tx_seq_counter = 1
        self.rx_seq_counter = 1
        self.ssl_context = ssl.create_default_context()
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.ssl_sock = self.ssl_context.wrap_socket(self.s, server_hostname=self.host)
        self.ssl_sock.connect((self.host, self.port))
        self.connected = True
        self.tx_thread_handle = threading.Thread(
            target=self.tx_thread, daemon=True, name="TX_THREAD"
        )
        self.rx_thread_handle = threading.Thread(
            target=self.rx_thread, daemon=True, name="RX_THREAD"
        )
        self.hb_thread_handle = threading.Thread(
            target=self.hb_thread, daemon=True, name="HEARTBEAT_THREAD"
        )
        with self.shutdown_lock:
            self.shutdown = False
        self.tx_thread_handle.start()
        self.rx_thread_handle.start()
        self.hb_thread_handle.start()

    def _close_connection(self):
        self.ssl_sock.shutdown(socket.SHUT_RDWR)
        self.connected = False
        with self.shutdown_lock:
            self.shutdown = True
        self.tx_thread_handle.join()
        self.rx_thread_handle.join()
        self.hb_thread_handle.join()
        self.ssl_sock.close()

    def login(self):
        self._initialize_connection()
        message = self._interface.generate_message("A")
        seq = self.increment_tx_counter()
        timestamp = (
            str(datetime.datetime.utcnow()).replace("-", "").replace(" ", "-")[:-3]
        )
        self._interface.add_tag(98, 0, message)
        self._interface.add_tag(108, 1, message)
        self._interface.add_tag(49, self.api_key, message.header)
        self._interface.add_tag(56, "Coinbase", message.header)
        self._interface.add_tag(554, self.passphrase, message)
        self._interface.add_tag(8013, "S", message)
        self._interface.add_tag(9406, "N", message)
        self._interface.add_tag(
            96,
            self._hmac_sign(
                timestamp, "A", str(seq), self.api_key, self.passphrase, self.secret
            ),
            message,
        )
        self._interface.prepare(message, seq, timestamp)
        self.tx_buffer.put(message)
        try:
            message = self.rx_buffer["A"].get(timeout=1)
        except Empty:
            try:
                message = self.rx_buffer["3"].get(timeout=1)
            except Empty:
                raise Exception("Login Failed. Unknown Error.")
            raise Exception(f"Login Failed, Reject Message Received: {message}")
        

    def logout(self):
        message = self._interface.generate_message("5")
        seq = self.increment_tx_counter()
        timestamp = (
            str(datetime.datetime.utcnow()).replace("-", "").replace(" ", "-")[:-3]
        )
        self._interface.add_tag(49, self.api_key, message.header)
        self._interface.add_tag(56, "Coinbase", message.header)
        self._interface.prepare(message, seq, timestamp)
        self.tx_buffer.put(message)
        try:
            message = self.rx_buffer["5"].get(timeout=1)
        except Empty:
            try:
                message = self.rx_buffer["3"].get(timeout=1)
            except Empty:
                raise Exception("Logout Failed. Unknown Error.")
            raise Exception(f"Login Failed, Reject Message Received: {message}")
        finally:
            self._close_connection()

    def _hmac_sign(self, t, msg_type, seq_num, api_key, password, secret):
        message = "\x01".join(
            [t, msg_type, seq_num, api_key, "Coinbase", password]
        ).encode("utf-8")
        hmac_key = base64.b64decode(secret)
        signature = hmac.new(hmac_key, message, hashlib.sha256)
        return base64.b64encode(signature.digest()).decode()

    def rx_thread(self):
        response = b""
        while True:
            net_response = self.ssl_sock.recv(4096)
            if len(net_response) == 0 or self.shutdown:
                print("RX SHUTDOWN")
                with self.shutdown_lock:
                    self.shutdown = True
                break
            response += net_response
            print(f"RECEIVED {response}")
            # Pull out msgs
            # Place on msg dict (of queues) with lock keeping accesses correct between threads.
            messages, response = self._interface.parse(response)
            for message in messages:
                assert (
                    int(message.header.tags["34"][0].get())
                    == self.increment_rx_counter()
                )
                self.rx_buffer[message.id_].put(message)

    def tx_thread(self):
        while True:
            try:
                message = self.tx_buffer.get(timeout=1)
            except Empty:
                if self.shutdown:
                    print("TX THREAD SHUTTING DOWN")
                    break
                else:
                    continue

            self.ssl_sock.sendall(message.fix_msg_payload)
            print(f"SENT {message.fix_msg_payload}")
            message = None

    def hb_thread(self):
        hb_req = None
        hb_msg = None
        while True:
            try:
                hb_req = self.rx_buffer["1"].get(timeout=0.1)
            except Empty:
                if self.shutdown:
                    print("HB THREAD SHUTTING DOWN")
                    break
            try:
                hb_msg = self.rx_buffer["0"].get(timeout=0.1)
            except Empty:
                if self.shutdown:
                    print("HB THREAD SHUTTING DOWN")
                    break
            if hb_req is not None:
                # Send a HB back to server.
                message = self.heartbeat(hb_req.tags["112"][0].get())
                self.tx_buffer.put(message)
            hb_req = None
            hb_msg = None

    def heartbeat(self, req_id=None):
        message = self._interface.generate_message("0")
        seq = self.increment_tx_counter()
        timestamp = (
            str(datetime.datetime.utcnow()).replace("-", "").replace(" ", "-")[:-3]
        )
        self._interface.add_tag(49, self.api_key, message.header)
        self._interface.add_tag(56, "Coinbase", message.header)
        if req_id is not None:
            self._interface.add_tag(112, req_id, message)
        self._interface.prepare(message, seq, timestamp)
        return message

    def increment_tx_counter(self):
        with self.tx_counter_lock:
            seq = self.tx_seq_counter
            self.tx_seq_counter += 1
        return seq

    def increment_rx_counter(self):
        with self.rx_counter_lock:
            seq = self.rx_seq_counter
            self.rx_seq_counter += 1
        return seq
