# -*- coding: utf-8 -*-
import subprocess
import time
import sys
import random

MESSAGE_PER_CLIENT = 10


def run_client(client_id):
    """各クライアント（サブプロセス）を起動して、負荷テストを実行"""
    username = f"user_{client_id}\n"

    process = subprocess.Popen(
        ["python3", "client.py"],
        stdin=subprocess.PIPE,
        # stdout=subprocess.DEVNULL,
        stdout=None,
        # stderr=subprocess.DEVNULL,
        stderr=None,
        text=True,
    )

    try:
        # 初回送信
        process.stdin.write("\n")  # サーバアドレス
        process.stdin.flush()
        time.sleep(0.1)

        process.stdin.write(username)
        process.stdin.flush()
        time.sleep(
            60
        )  # ユーザを登録したあとすべてのクライアント（サブプロセス）が起動するまで待つ

        # メッセージの繰り返し送信
        for i in range(MESSAGE_PER_CLIENT):
            message = f"test message {i}\n"
            process.stdin.write(message)
            process.stdin.flush()
            send_interval = random()
            time.sleep(send_interval)

    except Exception as e:
        print(f"Client {client_id} encountered an error: {e}")

    finally:
        time.sleep(30)
        process.stdin.write("exit\n")
        process.stdin.flush()
        process.stdin.close()


if __name__ == "__main__":
    CLIENT_ID = int(sys.argv[1])
    run_client(CLIENT_ID)
