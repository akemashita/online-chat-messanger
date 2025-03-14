# server.py
# stage1

import socket
import threading
import time
import queue
import asyncio


class UDPChatServer:
    def __init__(
        self,
        server_address="0.0.0.0",
        server_port=9001,
        session_lifetime=1800,
        check_interval=10,
        alert_message=20,
    ):
        ###########
        # 設定情報
        ###########
        self.debug_mode = False
        self.minimum_message = False
        self.adminname = "admin"
        self.server_address = server_address
        self.server_port = server_port
        self.session_lifetime_seconds = session_lifetime
        self.lifetime_check_interval_seconds = check_interval
        self.lifetime_alert_seconds = alert_message
        self.goodbye_message = "{} さんの接続が終了しました。"
        self.alert_message = "送信がない場合、残り {} 秒ほどで接続が終了します。"
        self.packet_count = 0
        self.semaphore = asyncio.Semaphore(100)  # 最大100並列処理
        self.lock = asyncio.Lock()

        # ソケット設定
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 2**24)
        self.sock.bind((server_address, server_port))
        self.sock.setblocking(False)

        # クライアントを管理する
        self.users_dict = {}

        # スレッド間通信用
        self.exit_user_queue = asyncio.Queue()
        self.message_queue = asyncio.Queue(maxsize=10000)

    async def start(self, debug_mode, minimum_message):
        """サーバを起動してクライアントからのメッセージを待つ"""
        self.debug_mode = debug_mode
        self.minimum_message = minimum_message
        print(f"starting up on port {self.server_address}:{self.server_port}")

        # ループを明示的に取得
        loop = asyncio.get_running_loop()

        # ソケットの読み込みイベントをループに追加
        loop.add_reader(self.sock.fileno(), self.receive_message)

        # タイムアウト監視をタスクとして開始
        asyncio.create_task(self.check_users_lifetime())

        # 非同期でメッセージを処理
        asyncio.create_task(self.process_incoming_packets())

        await asyncio.Event().wait()

    ##########
    # 以下、処理
    ##########
    def receive_message(self):
        """受信イベントで呼び出されるコールバック"""
        try:
            data, addr = self.sock.recvfrom(4096)
            self.packet_count += 1

            # キューが満杯になっても無視しない
            try:
                self.message_queue.put_nowait((data, addr))
            except asyncio.QueueFull:
                print("[WARNING] Message queue is full. Dropping packet.")

        except BlockingIOError:
            pass
        except Exception as e:
            print(f"[ERROR] {e}")

    async def process_incoming_packets(self):
        while True:
            data, addr = await self.message_queue.get()
            asyncio.create_task(self.handle_message(data, addr))

    async def send_data(self, data, addr):
        """非同期でソケットにデータを送信"""
        loop = asyncio.get_running_loop()

        if not self.minimum_message:
            print(f"[DEBUG] Sending data to {addr} following message: {data}", flush=True)
        await loop.run_in_executor(None, self.sock.sendto, data, addr)

    def show_users(self):
        """現在の接続ユーザと受信累計パケット数を表示"""
        print(f"[DEBUG] Active users: {len(self.users_dict)}, Received packets: {self.packet_count}")
        if not self.minimum_message:
            print(f"[DEBUG] users_dict:\n {self.users_dict}")

    async def check_user(self, username, address, message):
        """ユーザの存在を確認し、リストに追加する"""
        # 先に存在確認のみ行う
        if username in self.users_dict:
            if message == "exit":
                async with self.lock:
                    del self.users_dict[username]
                return True

            # メッセージを受信するたびに生存時間を戻す
            self.users_dict[username][1] = self.session_lifetime_seconds
            return False

        async with self.lock:
            self.users_dict[username] = [address, self.session_lifetime_seconds]
            if not self.minimum_message:
                self.show_users()
            return False


    async def check_users_lifetime(self):
        """クライアントの生存時間を監視するスレッド"""
        while True:
            self.show_users()
            await asyncio.sleep(self.lifetime_check_interval_seconds)

            # 辞書サイズが変わるため、キーをリストに変換してループ
            for username in list(self.users_dict.keys()):
                self.users_dict[username][1] -= self.lifetime_check_interval_seconds
                remaining_lifetime = self.users_dict[username][1]

                # タイムアウトしたユーザを削除
                if remaining_lifetime <= 0:
                    async with self.lock:
                        del self.users_dict[username]
                    await self.exit_user_queue.put(username)

                elif (
                    self.lifetime_check_interval_seconds
                    < remaining_lifetime
                    <= self.lifetime_alert_seconds
                ):
                    await self.send_alert(username)

    async def send_alert(self, username):
        """接続が切れそうなユーザに警告を送信"""
        adminname_bytes = self.adminname.encode("utf-8")
        adminname_len = len(adminname_bytes)
        alert_message_bytes = self.alert_message.format(
            self.lifetime_check_interval_seconds
        ).encode("utf-8")
        send_alert = bytes([adminname_len]) + adminname_bytes + alert_message_bytes

        await self.send_data(send_alert, self.users_dict[username][0])

        if self.debug_mode and not self.minimum_message:
            print(f"sent alert message to {self.users_dict[username][0]}")

    async def broadcast(self, data):
        """全クライアントにメッセージを送信"""
        current_users = self.users_dict.copy()
        number_current_users = len(current_users)
        for user, (addr, _) in current_users.items():
            await self.send_data(data, addr)
            await asyncio.sleep(0)

        if self.debug_mode and not self.minimum_message:
            print(f"sent message to {number_current_users} users")

    async def notify_exit(self, username):
        """ユーザ退出メッセージを全クライアントに送信"""
        goodbye_message_bytes = self.goodbye_message.format(username).encode("utf-8")
        adminname_bytes = self.adminname.encode("utf-8")
        adminname_len = len(adminname_bytes)
        send_data = bytes([adminname_len]) + adminname_bytes + goodbye_message_bytes

        await self.broadcast(send_data)

        if self.debug_mode and not self.minimum_message:
            print(f"[DEBUG] {username} has left the chat.")

    async def handle_message(self, data, address):
        """クライアントからのメッセージを受信して処理する"""
        async with self.semaphore:
            if not self.minimum_message:
                print("\nwaiting to receive message")
            if not data:
                return

            # バイト列をそのまま表示する
            if not self.minimum_message:
                print(f"[DEBUG] Raw received data: {data}")

            # 受信したメッセージを分解する（deserialize）
            # 最初の１バイトを username_len として読み取る
            username_len = data[0]

            # 次の username_len バイトがユーザ名
            username_bytes = data[1 : 1 + username_len]
            username = username_bytes.decode("utf-8")

            # 残りのバイトがメッセージ
            message_bytes = data[1 + username_len :]
            message = message_bytes.decode("utf-8")

            if not self.minimum_message:
                print(f"[DEBUG] Received bytes from {address}")
                print(f"  - Username: {username} (length: {username_len})")
                print(f"  - Message: {message}")

            # is_goodbye_message = self.check_user(username, address, message)
            # if is_goodbye_message:
            #     await self.notify_exit(username)
            # else:
            #     await self.broadcast(data)

            # ユーザ登録を並列処理として管理する
            # asyncio.create_task(self.register_user(username, address, message))
            tasks = [
                asyncio.create_task(self.check_user(username, address, message))
            ]
            results = await asyncio.gather(*tasks)
            is_goodbye_message = results[0]

            if is_goodbye_message:
                await self.notify_exit(username)
            else:
                await self.broadcast(data)

    # async def register_user(self, username, address, message):
    #     is_goodbye_message = await self.check_user(username, address, message)
    #     if is_goodbye_message:
    #         await self.notify_exit(username)


async def main():
    server = UDPChatServer()
    await server.start(debug_mode=True, minimum_message=True)

# サーバの起動
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
