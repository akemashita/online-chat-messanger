# -*- coding: utf-8 -*-
import threading
import time
import sys
import random
import socket

THREADS_PER_PROCESS = 20
MESSAGE_PER_CLIENT = 10

SERVER_ADDRESS = "127.0.0.1"
SERVER_PORT = 9001
SEND_REPORT_ADDRESS = "127.0.0.1"
SEND_REPORT_PORT = 9003
WAIT_BEFORE_MESSAGE_SEND = 80
SEND_INTERVAL_MIN = 20
SEND_INTERVAL_MAX = 60


def receive_loop(sock, local_count):
    """受信スレッド：受信したパケットをローカルカウンタで数え上げる"""
    time.sleep(WAIT_BEFORE_MESSAGE_SEND)

    sock.settimeout(1800)

    # 受信バッファのクリア（登録情報をカウントしない）
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 0)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)

    try:
        while True:
            try:
                _, _ = sock.recvfrom(4096)
                local_count[0] += 1
            except Exception as e:
                print(f"Receive error: {e}")
                break
    finally:
        sock.close()


def run_client(client_id, thread_id, local_count):
    """各クライアント（スレッド）を起動して、負荷テストを実行"""

    try:
        # UDPソケット作成
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # クライアント側のポートは OS に自動的に割り当ててもらう
        sock.bind(("", 0))

        # recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # recv_sock.bind(("", 0))

        recv_thread = threading.Thread(
            target=receive_loop, args=(sock, local_count), daemon=True
        )
        recv_thread.start()

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
            send_interval = random.uniform(SEND_INTERVAL_MIN, SEND_INTERVAL_MAX)
            time.sleep(send_interval)

            message = f"test message {i} from {username}\n"
            message_bytes = message.encode("utf-8")
            send_data = bytes([username_len]) + username_bytes + message_bytes
            sock.sendto(send_data, (SERVER_ADDRESS, SERVER_PORT))

    except Exception as e:
        print(f"Client {client_id} encountered an error: {e}")

    finally:
        # time.sleep(30)
        # message = "exit"
        # message_bytes = message.encode("utf-8")
        # send_data = bytes([username_len]) + username_bytes + message_bytes
        # sock.sendto(send_data, (SERVER_ADDRESS, SERVER_PORT))

        # 受信スレッドの終了を待機
        time.sleep(600)


def report_results(client_id, total_count):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        message = str(total_count).encode("utf-8")
        sock.sendto(message, (SEND_REPORT_ADDRESS, SEND_REPORT_PORT))
        time.sleep(1)
        sock.close()
        print(f"Client {client_id} reported {total_count} packets received.")
    except Exception as e:
        print(f"Reporting error: {e}")


def start_threads(client_id):
    """スレッドを生成して直接UDP通信"""
    threads = []
    local_counts = [[0] for _ in range(THREADS_PER_PROCESS)]

    for i in range(THREADS_PER_PROCESS):
        thread = threading.Thread(
            target=run_client, args=(client_id, i, local_counts[i])
        )
        thread.start()
        threads.append(thread)
        time.sleep(0.01)

    for thread in threads:
        thread.join()

    # スレッド単位のカウント合計を計算
    # print(f"local_counts:\n{local_counts}")
    total_count = sum(count[0] for count in local_counts)
    # print(f"total_counts: {total_count}")

    # パケット受信数を performance_test.py に送信
    report_results(client_id, total_count)


if __name__ == "__main__":
    CLIENT_ID = int(sys.argv[1])
    print(f"process {CLIENT_ID} 起動完了")
    start_threads(CLIENT_ID)
