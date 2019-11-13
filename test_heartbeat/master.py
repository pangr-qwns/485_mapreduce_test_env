import json
import os  # used to list files in directory
import shutil  # used to copy files
import socket  # create listener for heartbeat messages
import time
import threading

from worker import Worker


import socket # create listener for heartbeat messages
import os # used to list files in directory, get unique worker pid
import shutil  # used to copy files
import threading # graceful shutdown

class Master:
    def __init__(self, port_num):
        self.threads = []
        thread = threading.Thread(target=self.create_heartbeat_listener,
                                  args=(port_num - 1,))
        self.threads.append(thread)
        self.threads[0].start()

        thread2 = threading.Thread(target=self.count_inter_heartbeat_time)

        self.time_since_last_heartbeat = {0: 0}
        w = Worker(7999, 7998, 0)
        self.workers = [w]
        thread2.start()


    def handle_jobs(self, message):
        try:
            job = json.loads(message)
            message_type = job["message_type"]
        except KeyError:
            print ('json fail')
            return

        if message_type == "shutdown":
            print("shutdown")
            #self.shutdown(message_type)
        elif message_type == "register":
            print("register")
            #self.register(job)
        elif message_type == "new_master_job":
            print("new master job")
            #self.new_master_job(job)
        elif message_type == "status":
            print("status")
            #self.status(job)
        elif message_type == "heartbeat":
            self.heartbeat(job)


    def create_heartbeat_listener(self, port_num):
        """
        Create a UDP socket to listen for heartbeat messages.
        Ray
        :param port_num: tcp port number - 1
        :return:
        """
        sock = socket.socket(  # create socket
            socket.AF_INET,  # IPv4
            socket.SOCK_DGRAM,  # UDP
        )

        sock.bind(("localhost", port_num))

        while True:
            data, addr = sock.recvfrom(4096)
            print(data.decode('utf-8'))
            self.handle_jobs(data.decode('utf-8'))
            time.sleep(0.3)

        # how to send message using udp:
        # sock.sendto("Hello world".encode('utf-8'), ("localhost", 7999))


    def count_inter_heartbeat_time(self):
        """
        increment the val in time_since_last_heartbeat every second only if
        that worker is alive.
        if any val reaches 11,
            - kill the worker by updating the ready, busy, and dead
            dictionaries
            - redistribute the now dead worker's tasks.
        Ray
        :return: None
        """
        while True:
            for worker_id in self.time_since_last_heartbeat:
                if not self.workers[worker_id].dead:
                    self.time_since_last_heartbeat[worker_id] += 1
                    secs = self.time_since_last_heartbeat[worker_id]
                    if secs == 11:
                        print('Killing worker %i' % worker_id)
                        # kill the worker
                        self.workers[worker_id].dead = True
                        self.workers[worker_id].ready = False
                        if self.workers[worker_id].busy:
                            # redistribute tasks
                            print('redistribute tasks')
            print(self.time_since_last_heartbeat)
            time.sleep(1)


    def heartbeat(self, message):
        """
        look at message. set time_since_last_heartbeat[id] = 0 if that worker
        is alive.
        Ray
        """
        worker_id = message["worker_pid"]
        print("heartbeat")
        if not self.workers[worker_id].dead:
            self.time_since_last_heartbeat[worker_id] = 0


if __name__ == "__main__":
    m = Master(8000)