# client.py
# stage1

import socket
import threading
import queue
import sys
import readline

# スレッド間通信のためのキュー
send_queue = queue.Queue()
receive_queue = queue.Queue()


# 送受信を担当するスレッド
def send_receive_message(
    client_socket, username_len, username_bytes, send_queue, receive_queue
):
    sock = client_socket
    sock.setblocking(False)  # ソケットをノンブロッキングモードにする

    while True:
        try:
            #### メッセージ送信 ####
            if not send_queue.empty():
                own_message = send_queue.get()
                own_message_bytes = own_message.encode("utf-8")

                # 送信データを作成（serialize）
                send_data = bytes([username_len]) + username_bytes + own_message_bytes
                sent = sock.sendto(send_data, (server_address, server_port))
                # print(f"[DEBUG] send_data: {send_data}")

            #### メッセージ受信 ####
            try:
                # 受信メッセージを分解（deserialize）
                receive_data, _ = sock.recvfrom(4096)

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

                receive_queue.put([sender_username, sender_message])

            except BlockingIOError:
                # データがないときはスルー
                pass

        except Exception as e:
            print(f"Error: {e}")


# ユーザ入力を受け付けるスレッド
def input_message(send_queue, receive_queue):
    while True:
        # メッセージを入力させる
        message = input("\n> ")

        # 入力行を削除してサーバから受信した行のみを表示する
        sys.stdout.write(
            "\033[F\033[K\033[F"
        )  # １行上に移動、入力行をクリア、１行上に移動
        sys.stdout.flush()

        send_queue.put(message)

        if message.lower() == "exit":
            receive_queue.put(["exit", "exit"])
            break  # スレッドを終了


def print_message(sender, message):
    sys.stdout.write("\033[K")  # 現在の行をクリア
    print(f"\r[{sender}] {message}\n> {input_buffer()}", end="", flush=True)


# 入力途中の状態を取得する
def input_buffer():
    try:
        return readline.get_line_buffer()
    except ImportError:
        return ""


# メイン処理：受信メッセージの表示
if __name__ == "__main__":
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    server_address = input("Type in the server's address to connect to: ")
    server_port = 9001

    # クライアント側のポートは OS に自動的に割り当ててもらう
    address = ""
    port = 0
    sock.bind((address, port))

    # ユーザ名を入力させる
    username = input("ユーザ名を入力してください。\n> ")
    username_bytes = username.encode("utf-8")
    username_len = len(username_bytes)

    if username_len > 255:
        print("エラー：ユーザ名が長すぎます（最大 255 バイト）")
        exit(1)

    # 入力の案内を表示する
    print("### お知らせ ###")
    print("行頭に > が表示されているときは入力ができます。")
    print("終了したいときは exit と入力してください。")

    # 各スレッドを開始
    send_receive_thread = threading.Thread(
        target=send_receive_message,
        args=(sock, username_len, username_bytes, send_queue, receive_queue),
        daemon=True,
    )
    send_receive_thread.start()

    input_thread = threading.Thread(
        target=input_message, args=(send_queue, receive_queue), daemon=True
    )
    input_thread.start()

    loop = True
    try:
        while loop:
            # 受信メッセージがあれば表示
            while not receive_queue.empty():
                sender_username, receive_message = receive_queue.get()
                if sender_username == "exit" and receive_message == "exit":
                    loop = False
                    break
                else:
                    if username != sender_username:
                        print_message(sender_username, receive_message)
                    else:
                        print_message("YOU", receive_message)

    finally:
        print("\nclosing socket")
        sock.close()
