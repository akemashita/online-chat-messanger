# server.py
# stage1

import socket
import threading
import time
import queue

###########
# 設定情報
###########
session_lifetime_seconds = 30
lifetime_check_interval_seconds = 10
lifetime_alert_second = 20
goodbye_message = "{} さんの接続が終了しました。"
alert_message = "送信がない場合、残り {} 秒ほどで接続が終了します。"


##########
# 以下、処理
##########
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

server_address = "0.0.0.0"
server_port = 9001
print("starting up on port {}".format(server_port))

sock.bind((server_address, server_port))

# クライアントを管理する
users_dict = {}

# スレッド間通信用
exit_user_queue = queue.Queue()


def check_user(username, address, message):
    if not username in users_dict:
        users_dict[username] = [address, session_lifetime_seconds]
        print(f"[DEBUG] users_dict: {users_dict}")
        return False

    else:
        if message != "exit":
            users_dict[username][
                1
            ] = session_lifetime_seconds  # メッセージを受信するたびに生存時間を戻す

        else:
            del users_dict[username]
            return True


def check_users_lifetime(users_dict, exit_user_queue):
    """クライアントの生存時間を監視するスレッド"""
    while True:
        to_remove = []
        print(f"[DEBUG] users_dict:\n{users_dict}")
        for username in list(
            users_dict.keys()
        ):  # 辞書サイズが変わるため、キーをリストに変換してループ
            users_dict[username][1] -= lifetime_check_interval_seconds
            remaining_lifetime = users_dict[username][1]

            if remaining_lifetime <= 0:
                del users_dict[username]
                exit_user_queue.put(username)

            elif (lifetime_check_interval_seconds < remaining_lifetime) and (
                remaining_lifetime <= lifetime_alert_second
            ):
                adminname = "admin"
                adminname_bytes = adminname.encode("utf-8")
                adminname_len = len(adminname_bytes)
                alert_message_bytes = alert_message.format(
                    lifetime_check_interval_seconds
                ).encode("utf-8")
                send_alert = (
                    bytes([adminname_len]) + adminname_bytes + alert_message_bytes
                )

                sock.sendto(send_alert, users_dict[username][0])
                print("sent alert message to {}".format(users_dict[username][0]))

        # タイムアウトしたユーザを削除
        for username in to_remove:
            del users_dict[username]
            exit_user_queue.put(username)

        time.sleep(lifetime_check_interval_seconds)


# タイムアウト監視スレッドを開始
threading.Thread(
    target=check_users_lifetime, args=(users_dict, exit_user_queue), daemon=True
).start()


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

    is_goodbye_message = check_user(username, address, message)

    if data:
        if not is_goodbye_message:
            for key in users_dict:
                sent = sock.sendto(data, users_dict[key][0])
                print("sent {} bytes back to {}".format(sent, users_dict[key][0]))

        else:
            goodbye_message_bytes = goodbye_message.encode("utf-8")
            send_data = bytes([username_len]) + username_bytes + goodbye_message_bytes
            for key in users_dict:
                sent = sock.sendto(send_data, users_dict[key][0])
                print("sent {} bytes back to {}".format(sent, users_dict[key][0]))
