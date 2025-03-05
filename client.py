# client.py
# stage1

import socket

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

server_address = input("Type in the server's address to connect to: ")
server_port = 9001

# ユーザ名を入力させる
username = input("ユーザ名を入力してください。\n> ")

# クライアント側のポートは OS に自動的に割り当ててもらう
address = ""
port = 0
sock.bind((address, port))

try:
    while True:
        # メッセージを入力させる
        message = input("送信するメッセージを入力してください。\n> ").encode("utf-8")

        if not message:
            break  # からメッセージで終了

        sent = sock.sendto(message, (server_address, server_port))
        print("Send {} bytes".format(sent))

        print("waiting to receive")
        data, server = sock.recvfrom(4096)
        print("received: ", data.decode("utf-8"))

finally:
    print("closing socket")
    sock.close()
