# -*- coding: utf-8 -*-
import subprocess
import time
import socket
import threading

NUM_CLIENTS = 50
RECEIVE_REPORT_ADDRESS = "127.0.0.1"
RECEIVE_REPORT_PORT = 9003
received_count = 0
lock = threading.Lock()


def receive_results():
    global received_count
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((RECEIVE_REPORT_ADDRESS, RECEIVE_REPORT_PORT))
    sock.settimeout(1800)

    while True:
        try:
            data, addr = sock.recvfrom(4096)
            count = int(data.decode("utf-8"))
            with lock:
                received_count += count
        except socket.timeout:
            # 通信が途絶えたら終了
            print("Receiver timeout reached. Stopping receiver.")
            break
        except Exception as e:
            print(f"Receiver error: {e}")
            break

    sock.close()


def main():
    # 集計スレッドを起動
    receiver_thread = threading.Thread(target=receive_results, daemon=True)
    receiver_thread.start()

    # クライアントを並列に起動
    processes = []
    for i in range(NUM_CLIENTS):
        p = subprocess.Popen(
            ["python3", "performance_test_run_client.py", str(i)],
            stdin=subprocess.PIPE,
            # stdout=subprocess.DEVNULL,
            stdout=None,
            # stderr=subprocess.DEVNULL,
            stderr=None,
            # text=True,
        )
        processes.append(p)
        time.sleep(1)

    for p in processes:
        p.wait()

    time.sleep(1)

    # 結果表示
    print("負荷テスト完了！")
    number_of_clients = NUM_CLIENTS * 20  # 20 threds/process
    total_packets_to_server = number_of_clients * 10
    total_packets_from_server = (number_of_clients * 10) * number_of_clients
    received_rate = received_count / total_packets_from_server * 100
    print(f"- 全クライアント数： {number_of_clients}")
    print(f"- サーバへ送った全パケット数： {total_packets_to_server}")
    print(f"- サーバが送る全パケット数（理論値）： {total_packets_from_server}")
    print(f"- サーバから受け取った全パケット数（観測値）： {received_count}")
    print(f"- 受信率（観測値／理論値）: {received_rate:.1f} %")


if __name__ == "__main__":
    main()
