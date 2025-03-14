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
    print(f"Total Clients: {NUM_CLIENTS * 20}")
    print(f"Total Packets Sent: {NUM_CLIENTS * 20 * 10}")
    print(f"Total Packets Received: {received_count}")
    throughput = received_count / (NUM_CLIENTS * 20)
    print(f"Throughput: {throughput:.2f} packets/client")

if __name__ == "__main__":
    main()