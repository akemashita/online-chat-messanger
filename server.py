# server.py
# stage2

import socket
import threading
import time
import queue
import asyncio


class ServerState:
    def __init__(self, server_address, server_port, server_tcp_port):
        self.debug_mode = False
        self.minimum_message = False
        self.adminname = "admin"
        self.server_address = server_address
        self.server_port = server_port
        self.server_tcp_port = server_tcp_port
        self.session_lifetime_seconds = 1800
        self.lifetime_check_interval_seconds = 10
        self.lifetime_alert_seconds = 20
        self.goodbye_message = "{} さんの接続が終了しました。"
        self.alert_message = "送信がない場合、残り {} 秒ほどで接続が終了します。"

        # ソケット設定
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 2**24)
        self.sock.bind((server_address, server_port))
        self.sock.setblocking(False)
        self.tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_sock.bind(("", server_tcp_port))

        # クライアント管理
        self.users_dict = {}
        self.packet_count = 0

        # 非同期制御関係
        self.lock = asyncio.Lock()
        self.shutdown_event = asyncio.Event()
        self.message_queue = asyncio.Queue(maxsize=10000)
        self.exit_user_queue = asyncio.Queue()

        self.semaphore = asyncio.Semaphore(100)  # 最大100並列処理
        self.running = True


class ChatServer:
    def __init__(self, state):
        self.state = state
        self.state.tcp_sock.listen()
        self.rooms = {}  # { room_name: {"host": token, "members": [token]}}
        self.tokens = {}  # { token_bytes: username_str }

    def handle_tcp_connection(self):
        print(
            f"starting up on port {self.state.server_address}:{self.state.server_tcp_port}"
        )
        while True:
            conn, addr = self.state.tcp_sock.accept()
            print(f"[TCP] Connected by {addr}")
            data = conn.recv(1024)
            print(f"[TCP] Received: {data}")
            # TODO: TCRPの解析・トークン生成

            message = TCRPMessage.from_bytes(data)
            print("room: ", message.room_name)
            print("operation: ", message.operation)
            print("state: ", message.state)
            print("payload: ", message.payload_bytes)

            if (
                message.operation == 1 and message.state == 0
            ):  # state 0: create room, 1: receive request, 2: done
                import secrets

                username = message.payload_bytes.decode("utf-8")
                room = message.room_name.decode("utf-8")

                # トークン生成
                token = secrets.token_bytes(
                    32
                )  # 必要に応じて今後サイズを調整（最大255バイト）

                # ルーム新規作成
                if room not in self.rooms:
                    self.rooms[room] = {"host": token, "members": [token]}
                    self.tokens[token] = username

                    # 応答パケット作成
                    response = TCRPMessage(
                        room_name=room,
                        operation=1,
                        state=2,  # 完了
                        payload_bytes=token,
                    )

                    conn.sendall(response.to_bytes())
                    print(
                        f"[TCP] 新規ルームを作成しました。ルーム名：'{room}'、トークン：'{token.hex()[:8]}'"
                    )

                else:
                    print(f"[TCP] ルーム名 '{room}' はすでに存在しています。")
                    conn.sendall(b"ERROR: Room already exists")

            # 参加要求の場合の処理
            if message.operation == 2:
                import secrets

                username = message.payload_bytes.decode("utf-8")
                room = message.room_name.decode("utf-8")

                if room not in self.rooms:
                    err_msg = "ルーム名は存在しません。"
                    # err_msg = f"'{room}' is not found."

                    # 応答パケット作成
                    response = TCRPMessage(
                        room_name=room,
                        operation=2,
                        state=2,  # エラー：ルームが存在しない
                        payload_bytes=err_msg.encode("utf-8"),
                    )

                    conn.sendall(response.to_bytes())
                    print(f"[TCP] {err_msg}")
                    print(f"[TCP] {response.payload_bytes}")

                else:

                    # トークン生成
                    token = secrets.token_bytes(
                        32
                    )  # 必要に応じて今後サイズを調整（最大255バイト）

                    self.rooms[room]["members"].append(token)
                    self.tokens[token] = username

                    print(f"[DEBUG] rooms: {self.rooms}")
                    print(f"[DEBUG] tokens: {self.tokens}")

                    # 応答パケット作成
                    response = TCRPMessage(
                        room_name=room,
                        operation=2,
                        state=1,  # 成功
                        payload_bytes=token,
                    )

                    conn.sendall(response.to_bytes())
                    print(f"[TCP] ルーム名 '{room}' に参加します")

            conn.close()


class TCRPMessage:
    HEADER_SIZE = 32
    ROOM_NAME_MAX_LEN = 28
    PAYLOAD_MAX_LEN = 229
    OPERATION_SIZE_BYTES = 29

    def __init__(self, room_name, operation, state, payload_bytes):
        if isinstance(room_name, str):
            self.room_name = room_name.encode("utf-8")
        elif isinstance(room_name, bytes):
            self.room_name = room_name
        else:
            raise TypeError("room_name must be str or bytes")

        self.operation = operation
        self.state = state
        self.payload_bytes = payload_bytes

        if len(self.room_name) > self.ROOM_NAME_MAX_LEN:
            raise ValueError("Room name too long")
        if len(self.payload_bytes) > self.PAYLOAD_MAX_LEN:
            raise ValueError("Payload too long")

        print(f"[DEBUG] room_name as bytes: {self.room_name}")
        print(f"[DEBUG] payload as bytes: {self.payload_bytes}")

    def to_bytes(self):
        room_name_size = len(self.room_name)
        payload_bytes_size = len(self.payload_bytes)
        payload_bytes_size_bytes = payload_bytes_size.to_bytes(
            self.OPERATION_SIZE_BYTES, byteorder="big"
        )

        print(f"[DEBUG] payload_bytes_size as int: {payload_bytes_size}")
        print(f"[DEBUG] payload_bytes_size_bytes as bytes: {payload_bytes_size_bytes}")

        header = (
            bytes([room_name_size])
            + bytes([self.operation])
            + bytes([self.state])
            + payload_bytes_size_bytes
        )

        body = self.room_name + self.payload_bytes

        print(f"[DEBUG] header as bytes: {header}")
        print(f"[DEBUG] body as bytes: {body}")

        return header + body

    @classmethod
    def from_bytes(cls, data):
        if len(data) < cls.HEADER_SIZE:
            raise ValueError("Data too short for header")

        room_name_size = data[0]
        operation = data[1]
        state = data[2]
        payload_size = int.from_bytes(data[3:32], byteorder="big")

        room_name_start = cls.HEADER_SIZE
        room_name_end = room_name_start + room_name_size
        room_name = data[room_name_start:room_name_end]

        payload = data[room_name_end : room_name_end + payload_size]

        return cls(room_name.decode("utf-8"), operation, state, payload)


class UDPChatServer:
    def __init__(self, state):
        self.state = state

    async def start(self, debug_mode=False, minimum_message=False):
        """サーバを起動してクライアントからのメッセージを待つ"""
        self.state.debug_mode = debug_mode
        self.state.minimum_message = minimum_message
        print(
            f"starting up on port {self.state.server_address}:{self.state.server_port}"
        )

        # ループを明示的に取得
        loop = asyncio.get_running_loop()

        # ソケットの読み込みイベントをループに追加
        loop.add_reader(self.state.sock.fileno(), self.receive_message)

        # タイムアウト監視をタスクとして開始
        asyncio.create_task(self.check_users_lifetime())

        # 非同期でメッセージを処理
        asyncio.create_task(self.process_incoming_packets())

        # 非同期でサーバ終了用 exit コマンドを処理
        asyncio.create_task(self.handle_exit_command())

        # イベントループを維持する
        await self.state.shutdown_event.wait()

        self.state.sock.close()
        print("server socket closed.")

    def receive_message(self):
        """受信イベントで呼び出されるコールバック"""
        try:
            data, addr = self.state.sock.recvfrom(4096)
            self.state.packet_count += 1

            # キューが満杯になっても無視しない
            try:
                self.state.message_queue.put_nowait((data, addr))
            except asyncio.QueueFull:
                print("[WARNING] Message queue is full. Dropping packet.")

        except BlockingIOError:
            pass
        except Exception as e:
            print(f"[ERROR] {e}")

    async def process_incoming_packets(self):
        while True:
            data, addr = await self.state.message_queue.get()
            asyncio.create_task(self.handle_message(data, addr))

    async def handle_exit_command(self):
        """サーバ用終了コマンド exit を処理する"""
        loop = asyncio.get_running_loop()
        while self.state.running:
            command = await loop.run_in_executor(
                None, input, "Enter 'exit' to stop the server.\n"
            )
            if command.lower() == "exit":
                print("stopping the server...")
                self.state.running = False
                self.state.shutdown_event.set()
                break

    async def handle_message(self, data, address):
        """クライアントからのメッセージを受信して処理する"""
        async with self.state.semaphore:
            if not self.state.minimum_message:
                print("\nwaiting to receive message")
            if not data:
                return

            # バイト列をそのまま表示する
            if not self.state.minimum_message:
                print(f"[DEBUG] Raw received data: {data}")

            # TODO:トークン検証とリレー処理
            # 受信したメッセージを分解する（deserialize）
            # 最初の１バイトを username_len として読み取る
            username_len = data[0]

            # 次の username_len バイトがユーザ名
            username_bytes = data[1 : 1 + username_len]
            username = username_bytes.decode("utf-8")

            # 残りのバイトがメッセージ
            message_bytes = data[1 + username_len :]
            message = message_bytes.decode("utf-8")

            if not self.state.minimum_message:
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
            tasks = [asyncio.create_task(self.check_user(username, address, message))]
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

    async def broadcast(self, data):
        """全クライアントにメッセージを送信"""
        current_users = self.state.users_dict.copy()
        number_current_users = len(current_users)
        for user, (addr, _) in current_users.items():
            await self.send_data(data, addr)
            await asyncio.sleep(0)

        if self.state.debug_mode and not self.state.minimum_message:
            print(f"sent message to {number_current_users} users")

    async def send_data(self, data, addr):
        """非同期でソケットにデータを送信"""
        loop = asyncio.get_running_loop()

        if not self.state.minimum_message:
            print(
                f"[DEBUG] Sending data to {addr} following message: {data}", flush=True
            )
        await loop.run_in_executor(None, self.state.sock.sendto, data, addr)

    def show_users(self):
        """現在の接続ユーザと受信累計パケット数を表示"""
        print(
            f"[DEBUG] Active users: {len(self.state.users_dict)}, Received packets: {self.state.packet_count}"
        )
        if not self.state.minimum_message:
            print(f"[DEBUG] users_dict:\n {self.state.users_dict}")

    async def check_user(self, username, address, message):
        """ユーザの存在を確認し、リストに追加する"""
        # 先に存在確認のみ行う
        if username in self.state.users_dict:
            if message == "exit":
                async with self.state.lock:
                    del self.state.users_dict[username]
                return True

            # メッセージを受信するたびに生存時間を戻す
            self.state.users_dict[username][1] = self.state.session_lifetime_seconds
            return False

        async with self.state.lock:
            self.state.users_dict[username] = [
                address,
                self.state.session_lifetime_seconds,
            ]
            if not self.state.minimum_message:
                self.show_users()
            return False

    async def check_users_lifetime(self):
        """クライアントの生存時間を監視するスレッド"""
        while True:
            self.show_users()
            await asyncio.sleep(self.state.lifetime_check_interval_seconds)

            # 辞書サイズが変わるため、キーをリストに変換してループ
            for username in list(self.state.users_dict.keys()):
                self.state.users_dict[username][
                    1
                ] -= self.state.lifetime_check_interval_seconds
                remaining_lifetime = self.state.users_dict[username][1]

                # タイムアウトしたユーザを削除
                if remaining_lifetime <= 0:
                    async with self.state.lock:
                        del self.state.users_dict[username]
                    await self.state.exit_user_queue.put(username)

                elif (
                    self.state.lifetime_check_interval_seconds
                    < remaining_lifetime
                    <= self.state.lifetime_alert_seconds
                ):
                    await self.send_alert(username)

    async def send_alert(self, username):
        """接続が切れそうなユーザに警告を送信"""
        adminname_bytes = self.state.adminname.encode("utf-8")
        adminname_len = len(adminname_bytes)
        alert_message_bytes = self.state.alert_message.format(
            self.state.lifetime_check_interval_seconds
        ).encode("utf-8")
        send_alert = bytes([adminname_len]) + adminname_bytes + alert_message_bytes

        await self.send_data(send_alert, self.state.users_dict[username][0])

        if self.state.debug_mode and not self.state.minimum_message:
            print(f"sent alert message to {self.state.users_dict[username][0]}")

    async def notify_exit(self, username):
        """ユーザ退出メッセージを全クライアントに送信"""
        goodbye_message_bytes = self.state.goodbye_message.format(username).encode(
            "utf-8"
        )
        adminname_bytes = self.state.adminname.encode("utf-8")
        adminname_len = len(adminname_bytes)
        send_data = bytes([adminname_len]) + adminname_bytes + goodbye_message_bytes

        await self.broadcast(send_data)

        if self.state.debug_mode and not self.state.minimum_message:
            print(f"[DEBUG] {username} has left the chat.")


async def run_tcp_loop(tcp_server):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, tcp_server.handle_tcp_connection)


async def main():
    state = ServerState(
        server_address="0.0.0.0", server_port=9001, server_tcp_port=9101
    )
    server = UDPChatServer(state)
    server_tcp = ChatServer(state)

    # UDP + TCP を並列で実行
    await asyncio.gather(
        server.start(debug_mode=True, minimum_message=True), run_tcp_loop(server_tcp)
    )


# サーバの起動
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
