# client.py
# stage1

import socket

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

server_address = input("Type in the server's address to connect to: ")
server_port = 9001

address = ""
port = 9050
message = b"Message to send to the client."

sock.bind((address, port))

try:
    print("sending {!r}".format(message))

    sent = sock.sendto(message, (server_address, server_port))
    print("Send {} bytes".format(sent))

    print("waiting to receive")
    data, server = sock.recvfrom(4096)
    print("received {!r}".format(data))

finally:
    print("closing socket")
    sock.close()
