# client.py
# stage2

import socket
import threading
import queue
import sys
import readline
import time


class State:
    def __init__(self, server_ip, server_tcp_port):
        self.server_ip = server_ip
        self.server_tcp_port = server_tcp_port

        # 状態管理用の変数
        self.username = None
        self.token = None
        self.room_name = None

    def set_username(self, name):
        self.username = name

    def set_token(self, token):
        self.token = token

    def set_room_name(self, room_name):
        self.room_name = room_name

    def get_identity_info(self):
        return {
            "username": self.username,
            "token": self.token,
            "room_name": self.room_name,
        }


class Client:
    def __init__(self, state):
        self.state = state

    def tcp_request(self, request_data):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.state.server_ip, self.state.server_tcp_port))
            sock.sendall(request_data)
            response = sock.recv(1024)
            print(f"[TCP] Response: {response}")
            return response


class UDPClient:
    def __init__(
        self,
        server_address="0.0.0.0",
        server_port=9001,
        client_address="",
        client_port=0,
        username="no name",
        username_bytes=7,
        username_len=7,
    ):
        ###########
        # 設定情報
        ###########
        self.server_address = server_address
        self.server_port = server_port
        self.client_address = client_address
        self.client_port = client_port
        self.username = username
        self.username_bytes = username_bytes
        self.username_len = username_len
        self.thread_running = True

        # ソケット設定
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # クライアント側のポートは OS に自動的に割り当ててもらう
        self.sock.bind((self.client_address, self.client_port))

        # スレッド間通信のためのキュー
        self.send_queue = queue.Queue()
        self.receive_queue = queue.Queue()

    def start(self):
        """クライアントを起動して、メッセージの送受信を行う"""
        # 案内を表示する
        print("### お知らせ ###")
        print("行頭に > が表示されているときは入力ができます。")
        print("終了したいときは exit と入力してください。")

        # 各スレッドを開始する
        send_receive_thread = threading.Thread(
            target=self.send_receive_message,
            daemon=True,
        )
        send_receive_thread.start()

        input_thread = threading.Thread(target=self.input_message, daemon=True)
        input_thread.start()

        loop = True
        try:
            while loop:
                # 受信メッセージがあれば表示
                while not self.receive_queue.empty():
                    sender_username, receive_message = self.receive_queue.get()
                    if sender_username == "exit" and receive_message == "exit":
                        loop = False
                        break
                    else:
                        if username != sender_username:
                            self.print_message(sender_username, receive_message)
                        else:
                            self.print_message("YOU", receive_message)

        finally:
            self.thread_running = False
            print("\nstop threads")
            send_receive_thread.join()
            input_thread.join()
            print("closing socket")
            self.sock.close()

    def send_receive_message(self):
        """送受信を担当するスレッド"""
        self.sock.setblocking(False)  # ソケットをノンブロッキングモードにする

        while self.thread_running:
            try:
                time.sleep(0.1)

                #### メッセージ送信 ####
                if not self.send_queue.empty():
                    own_message = self.send_queue.get()
                    own_message_bytes = own_message.encode("utf-8")

                    # 送信データを作成（serialize）
                    send_data = (
                        bytes([username_len]) + self.username_bytes + own_message_bytes
                    )

                    # 送信データが4096バイトを超えていなければサーバに送る
                    if not len(send_data) > 4096:
                        sent = self.sock.sendto(
                            send_data, (self.server_address, self.server_port)
                        )
                        # print(f"[DEBUG] send_data: {send_data}")
                    else:
                        print("エラー：入力が多すぎます。もう少し減らしてください")

                #### メッセージ受信 ####
                try:
                    # 受信メッセージを分解（deserialize）
                    receive_data, _ = self.sock.recvfrom(4096)

                    if len(receive_data) < 2:
                        continue

                    # 最初の１バイトを sender_username_len として読み取る
                    sender_username_len = receive_data[0]

                    if len(receive_data) < 1 + sender_username_len:
                        continue

                    # 次の sender_username_len バイトがユーザ名
                    sender_username_bytes = receive_data[1 : 1 + sender_username_len]
                    sender_username = sender_username_bytes.decode("utf-8")

                    # 残りのバイトがメッセージ
                    sender_message_bytes = receive_data[1 + sender_username_len :]
                    sender_message = sender_message_bytes.decode("utf-8")

                    self.receive_queue.put([sender_username, sender_message])

                except BlockingIOError:
                    # データがないときはスルー
                    pass

            except Exception as e:
                print(f"Error: {e}")

    def input_message(self):
        """ユーザ入力を受け付けるスレッド"""
        while self.thread_running:
            # メッセージを入力させる
            message = input("\n> ")

            # 入力行を削除してサーバから受信した行のみを表示する
            sys.stdout.write(
                "\033[F\033[K\033[F"
            )  # １行上に移動、入力行をクリア、１行上に移動
            sys.stdout.flush()

            if not message:
                # 入力がなかった場合はサーバに送信しない
                continue

            self.send_queue.put(message)

            if message.lower() == "exit":
                self.receive_queue.put(["exit", "exit"])
                break  # スレッドを終了

    def print_message(self, sender, message):
        """メッセージを表示して、その後に入力行を表示する"""
        sys.stdout.write("\033[K")  # 現在の行をクリア
        print(f"\r[{sender}] {message}\n> {self.input_buffer()}", end="", flush=True)

    def input_buffer(self):
        """入力途中の状態を取得する"""
        try:
            return readline.get_line_buffer()
        except ImportError:
            return ""


# メイン処理：受信メッセージの表示
if __name__ == "__main__":
    # TCPでなにか送ってみる
    client_state = State("127.0.0.1", 9101)
    tmp_room_name = "default"
    tmp_room_name_bytes = tmp_room_name.encode("utf-8")
    tmp_token = "dummy_token_123"
    tmp_username = "あけました"

    # ユーザ入力の代わり
    client_state.set_username(tmp_username)
    client_state.set_token(tmp_token)
    client_state.set_room_name(tmp_room_name)

    # 確認
    print("現在の状態:", client_state.get_identity_info())

    payload_bytes = b"Hello TCP"
    room_name_size = len(tmp_room_name)
    payload_size = len(payload_bytes)
    payload_size_bytes = payload_size.to_bytes(29, byteorder="big")
    operation = 1
    tcrp_state = 0

    header = (
        bytes([room_name_size])
        + bytes([operation])
        + bytes([tcrp_state])
        + payload_size_bytes
    )

    body = tmp_room_name_bytes + payload_bytes

    tcp_client = Client(client_state)
    tcp_client.tcp_request(header + body)

    server_address = input("Type in the server's address to connect to: ")
    server_port = 9001

    # ユーザ名を入力させる
    username = input("ユーザ名を入力してください。\n> ")
    username_bytes = username.encode("utf-8")
    username_len = len(username_bytes)

    if username_len > 255:
        print("エラー：ユーザ名が長すぎます（最大 255 バイト）")
        exit(1)

    client = UDPClient(
        server_address=server_address,
        server_port=server_port,
        username=username,
        username_bytes=username_bytes,
        username_len=username_len,
    )
    client.start()
