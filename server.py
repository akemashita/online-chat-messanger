# server.py
# stage1

import socket

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

server_address = "0.0.0.0"
server_port = 9001
print("starting up on port {}".format(server_port))

sock.bind((server_address, server_port))

# クライアントからの接続を待つ処理
while True:
    print("\nwaiting to receive message")
    data, address = sock.recvfrom(4096)

    # バイト列をそのまま表示する
    print(f"Raw received data: {data}")

    # 受信したメッセージを分解する（deserialize）
    # 最初の１バイトを username_len として読み取る
    username_len = data[0]

    # 次の username_len バイトがユーザ名
    username_bytes = data[1 : 1 + username_len]
    username = username_bytes.decode("utf-8")

    # 残りのバイトがメッセージ
    message_bytes = data[1 + username_len :]
    message = message_bytes.decode("utf-8")

    print(f"Received bytes from {address}")
    print(f"  - Username: {username} (length: {username_len})")
    print(f"  - Message: {message}")

    if data:
        sent = sock.sendto(data, address)
        print("sent {} bytes back to {}".format(sent, address))
