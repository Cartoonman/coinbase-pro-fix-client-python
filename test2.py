import socket
import base64
import time, datetime
import hmac
import hashlib
import ssl
from coinbasepro_fix_client import *
import os

PASSPHRASE = os.environ.get("CBPFIX_PASSPHRASE")
API_KEY = os.environ.get("CBPFIX_API_KEY")
API_SECRET = os.environ.get("CBPFIX_API_SECRET")

# Based on https://stackoverflow.com/questions/47110454/how-to-send-fix-logon-message-with-python-to-gdax/

host = "fix.pro.coinbase.com"
port = 4198
context = ssl.create_default_context()
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
ssl_sock = context.wrap_socket(s, server_hostname=host)
ssl_sock.connect((host, port))


def check_sum(s):
    sum = 0
    for char in s:
        sum += ord(char)
    sum = str(sum % 256)
    while len(sum) < 3:
        sum = "0" + sum
    return sum


def sign(t, msg_type, seq_num, api_key, password, secret):
    message = "\x01".join([t, msg_type, seq_num, api_key, "Coinbase", password]).encode(
        "utf-8"
    )
    hmac_key = base64.b64decode(secret)
    signature = hmac.new(hmac_key, message, hashlib.sha256)
    return base64.b64encode(signature.digest()).decode()


def wrap_fix_string(msg_type, body):
    bodyLength = len("35={}|".format(msg_type)) + len(body)
    header = "8=FIX.4.2|9=00{}|35={}|".format(bodyLength, msg_type)
    msg = header + body
    return msg


def generate_login_string(seq_num, t, api_key, password, secret):
    msgType = "A"
    sign_b64 = sign(t, msgType, seq_num, api_key, password, secret)
    body = f"49={api_key}|554={password}|96={sign_b64}|8013=S|52={t}|56=Coinbase|98=0|108=30|34={seq_num}|9406=N|"  # using the same time (t) and seq_num as in signed text
    msg = wrap_fix_string(msgType, body)
    msg = msg.replace("|", "\x01")
    c_sum = check_sum(msg)
    return msg + "10={}\x01".format(c_sum)


seq_num = "1"
t = str(datetime.datetime.utcnow()).replace("-", "").replace(" ", "-")[:-3]
logon = generate_login_string(seq_num, t, API_KEY, PASSPHRASE, API_SECRET)

context = FIXContext("4.2_coinbase")
msg_bldr = FIXMessageBuilder(context)
print("SENDING")
[
    print(
        msg_bldr.decode(x.split("=")[0])
        + "("
        + x.split("=")[0]
        + ") = "
        + x.split("=")[1]
    )
    for x in logon.split("\x01")[:-1]
]

logon = logon.encode("ascii")
ssl_sock.sendall(logon)
print("GETTING")
response = ssl_sock.recv(4096)
print(response)
response = response.decode("ascii")
print(response)
[
    print(
        msg_bldr.decode(x.split("=")[0])
        + "("
        + x.split("=")[0]
        + ") = "
        + x.split("=")[1]
    )
    for x in response.split("\x01")[:-1]
]
