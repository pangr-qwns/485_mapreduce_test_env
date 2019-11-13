import threading
import socket
import json
import time

class Worker:
    def __init__(self, id, send_port, rec_port):
        self.ready = True
        self.dead = False
        self.busy = False
        self.id = id
        self.send_port = send_port
        self.rec_port = rec_port
        listen_thread = threading.Thread(target=self.listen, args=(rec_port,))
        listen_thread.start()


    def listen(self, port_num):
        # Create an INET, STREAMing socket, this is TCP
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Bind the socket to the server
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("localhost", port_num))
        sock.listen(5)

        # Connect to a client
        clientsocket, address = sock.accept()
        print("Connection from", address[0])

        # Receive data, one chunk at a time.  When the client closes the
        # connection, recv() returns empty data, which breaks out of the loop.  We
        # make a simplifying assumption that the client will always cleanly close
        # the connection.
        message_chunks = []
        while True:
            data = clientsocket.recv(4096)
            if not data:
                break
            message_chunks.append(data)
        #clientsocket.close()

        message_bytes = b''.join(message_chunks)
        message_str = message_bytes.decode("utf-8")
        message_dict = json.loads(message_str)
        print(message_dict)


    def send_message(self):
        pass