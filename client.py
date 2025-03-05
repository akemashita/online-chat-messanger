# client.py
# stage1

import socket

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

server_address = input("Type in the server's address to connect to: ")
server_port = 9001

# ユーザ名を入力させる
username = input("ユーザ名を入力してください。\n> ")
username_bytes = username.encode("utf-8")
username_len = len(username_bytes)

if username_len > 255:
    print("エラー：ユーザ名が長すぎます（最大 255 バイト）")
    exit(1)

# クライアント側のポートは OS に自動的に割り当ててもらう
address = ""
port = 0
sock.bind((address, port))

try:
    while True:
        # メッセージを入力させる
        message = input("送信するメッセージを入力してください。（exit で終了）：\n> ")
        if message.lower() == "exit":
            break

        message_bytes = message.encode("utf-8")

        # 送信データを作成（serialize）
        send_data = bytes([username_len]) + username_bytes + message_bytes

        sent = sock.sendto(send_data, (server_address, server_port))
        print("Send {} bytes".format(sent))

        print("waiting to receive")
        received_data, server = sock.recvfrom(4096)
        print("received: ", received_data.decode("utf-8"))

finally:
    print("closing socket")
    sock.close()
