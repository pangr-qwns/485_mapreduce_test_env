import json
import os  # used to list files in directory
import shutil  # used to copy files
import socket  # create listener for heartbeat messages
import time
import threading



import socket # create listener for heartbeat messages
import os # used to list files in directory, get unique worker pid
import shutil  # used to copy files
import threading # graceful shutdown


class Worker:
    def __init__(self, send_port, rec_port, pid):
        self.ready = True
        self.busy = False
        self.dead = False

        self.rec_port = rec_port
        self.pid = pid
        self.threads = []
        thread = threading.Thread(target=self.create_heartbeat,
                                  args=(send_port,))
        self.threads.append(thread)
        self.threads[0].start()

    def create_heartbeat(self, master_port):
        sock = socket.socket(
            socket.AF_INET,  # Internet
            socket.SOCK_DGRAM
        )  # UDP

        while True:
            heartbeat = '{"message_type": "heartbeat", ' \
                        '"worker_pid": %i}' % self.pid
            sock.sendto(heartbeat.encode('utf-8'), ("localhost", master_port))
            time.sleep(2)
