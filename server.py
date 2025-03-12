# server.py
# stage1

import socket
import threading
import time
import queue


class UDPChatServer:
    def __init__(
        self,
        server_address="0.0.0.0",
        server_port=9001,
        session_lifetime=120,
        check_interval=10,
        alert_message=20,
    ):
        ###########
        # 設定情報
        ###########
        self.debug_mode = False
        self.adminname = "admin"
        self.server_address = server_address
        self.server_port = server_port
        self.session_lifetime_seconds = session_lifetime
        self.lifetime_check_interval_seconds = check_interval
        self.lifetime_alert_seconds = alert_message
        self.goodbye_message = "{} さんの接続が終了しました。"
        self.alert_message = "送信がない場合、残り {} 秒ほどで接続が終了します。"

        # ソケット設定
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((server_address, server_port))

        # クライアントを管理する
        self.users_dict = {}

        # スレッド間通信用
        self.exit_user_queue = queue.Queue()

    def start(self, debug_mode):
        """サーバを起動してクライアントからのメッセージを待つ"""
        self.debug_mode = debug_mode
        print(f"starting up on port {self.server_address}:{self.server_port}")

        # タイムアウト監視スレッドを起動する
        threading.Thread(target=self.check_users_lifetime, daemon=True).start()

        while True:
            self.receive_message()

    ##########
    # 以下、処理
    ##########
    def show_users(self):
        # print(f"[DEBUG] users_dict:\n {self.users_dict}")
        print(f"[DEBUG] Active users: {len(self.users_dict)}")

    def check_user(self, username, address, message):
        """ユーザの存在を確認し、リストに追加する"""
        if not username in self.users_dict:
            self.users_dict[username] = [address, self.session_lifetime_seconds]
            self.show_users()
            return False

        else:
            if message != "exit":
                self.users_dict[username][
                    1
                ] = (
                    self.session_lifetime_seconds
                )  # メッセージを受信するたびに生存時間を戻す

            else:
                del self.users_dict[username]
                return True

    def check_users_lifetime(self):
        """クライアントの生存時間を監視するスレッド"""
        while True:
            self.show_users()

            time.sleep(self.lifetime_check_interval_seconds)

            for username in list(
                self.users_dict.keys()
            ):  # 辞書サイズが変わるため、キーをリストに変換してループ
                self.users_dict[username][1] -= self.lifetime_check_interval_seconds
                remaining_lifetime = self.users_dict[username][1]

                # タイムアウトしたユーザを削除
                if remaining_lifetime <= 0:
                    del self.users_dict[username]
                    self.exit_user_queue.put(username)

                elif (
                    self.lifetime_check_interval_seconds
                    < remaining_lifetime
                    <= self.lifetime_alert_seconds
                ):
                    self.send_alert(username)

    def send_alert(self, username):
        """接続が切れそうなユーザに警告を送信"""
        adminname_bytes = self.adminname.encode("utf-8")
        adminname_len = len(adminname_bytes)
        alert_message_bytes = self.alert_message.format(
            self.lifetime_check_interval_seconds
        ).encode("utf-8")
        send_alert = bytes([adminname_len]) + adminname_bytes + alert_message_bytes

        self.sock.sendto(send_alert, self.users_dict[username][0])
        if self.debug_mode:
            print(f"sent alert message to {self.users_dict[username][0]}")

    def broadcast(self, data):
        """全クライアントにメッセージを送信"""
        current_users = self.users_dict.copy()
        number_current_users = len(current_users)
        for user, (addr, _) in current_users.items():
            self.sock.sendto(data, addr)
            # print(f"sent message to {addr}")
        if self.debug_mode:
            print(f"sent message to {number_current_users} users")

    def notify_exit(self, username):
        """ユーザ退出メッセージを全クライアントに送信"""
        goodbye_message_bytes = self.goodbye_message.format(username).encode("utf-8")
        adminname_bytes = self.adminname.encode("utf-8")
        adminname_len = len(adminname_bytes)
        send_data = bytes([adminname_len]) + adminname_bytes + goodbye_message_bytes

        self.broadcast(send_data)
        if self.debug_mode:
            print(f"[DEBUG] {username} has left the chat.")

    def receive_message(self):
        """クライアントからのメッセージを受信して処理する"""
        # print("\nwaiting to receive message")
        data, address = self.sock.recvfrom(4096)

        if not data:
            return

        # バイト列をそのまま表示する
        # print(f"[DEBUG] Raw received data: {data}")

        # 受信したメッセージを分解する（deserialize）
        # 最初の１バイトを username_len として読み取る
        username_len = data[0]

        # 次の username_len バイトがユーザ名
        username_bytes = data[1 : 1 + username_len]
        username = username_bytes.decode("utf-8")

        # 残りのバイトがメッセージ
        message_bytes = data[1 + username_len :]
        message = message_bytes.decode("utf-8")

        # print(f"[DEBUG] Received bytes from {address}")
        # print(f"  - Username: {username} (length: {username_len})")
        # print(f"  - Message: {message}")

        is_goodbye_message = self.check_user(username, address, message)

        if is_goodbye_message:
            self.notify_exit(username)
        else:
            self.broadcast(data)


# サーバの起動
if __name__ == "__main__":
    debug_mode = True

    server = UDPChatServer()
    server.start(debug_mode)
