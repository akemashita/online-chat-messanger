# -*- coding: utf-8 -*-
import threading
import time
import sys
import random
import socket

THREADS_PER_PROCESS = 25
MESSAGE_PER_CLIENT = 10

SERVER_ADDRESS = "0.0.0.0"
SERVER_PORT = 9001
SEND_INTERVAL_MIN = 0.01
SEND_INTERVAL_MAX = 0.5
WAIT_BEFORE_MESSAGE_SEND = 30


def run_client(client_id, thread_id):
    """各クライアント（スレッド）を起動して、負荷テストを実行"""

    try:
        # UDPソケット作成
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # クライアント側のポートは OS に自動的に割り当ててもらう
        sock.bind(("", 0))

        # 初回送信
        username = f"user_{client_id}_{thread_id}"
        username_bytes = username.encode("utf-8")
        username_len = len(username_bytes)
        time.sleep(5)

        message = f"first message from {username}\n"
        message_bytes = message.encode("utf-8")
        send_data = bytes([username_len]) + username_bytes + message_bytes
        sock.sendto(send_data, (SERVER_ADDRESS, SERVER_PORT))

        # 全クライアントが起動し終わるまで待機
        time.sleep(WAIT_BEFORE_MESSAGE_SEND)

        # メッセージの繰り返し送信
        for i in range(MESSAGE_PER_CLIENT):
            message = f"test message {i} from {username}\n"
            message_bytes = message.encode("utf-8")
            send_data = bytes([username_len]) + username_bytes + message_bytes
            sock.sendto(send_data, (SERVER_ADDRESS, SERVER_PORT))

            send_interval = random.uniform(SEND_INTERVAL_MIN, SEND_INTERVAL_MAX)
            time.sleep(send_interval)

    except Exception as e:
        print(f"Client {client_id} encountered an error: {e}")

    finally:
        time.sleep(30)
        message = "exit"
        message_bytes = message.encode("utf-8")
        send_data = bytes([username_len]) + username_bytes + message_bytes
        sock.sendto(send_data, (SERVER_ADDRESS, SERVER_PORT))
        sock.close()


def start_threads(client_id):
    """100スレッドを生成して直接UDP通信"""
    threads = []

    for i in range(THREADS_PER_PROCESS):
        thread = threading.Thread(target=run_client, args=(client_id, i))
        thread.start()
        threads.append(thread)
        time.sleep(0.01)

    for thread in threads:
        thread.join()


if __name__ == "__main__":
    CLIENT_ID = int(sys.argv[1])
    print(f"process {CLIENT_ID} 起動完了")
    start_threads(CLIENT_ID)
