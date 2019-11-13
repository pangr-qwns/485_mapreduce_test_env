import os
import threading
import socket
from pprint import pprint
from worker import Worker
import json

class Master:
    def __init__(self, port_num):
        w = Worker(0, 6000, 6001)
        w2 = Worker(1, 6002, 6003)
        wdead = Worker(2, 6004, 6005)
        wdead.dead = True
        self.workers = [w, w2, wdead]
        self.send_message_for_group()


    def send_tcp_message(self, dict):
        wid = dict["worker_pid"]

        # create an INET, STREAMing socket, this is TCP
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # connect to the server
        sock.connect(("localhost", self.workers[wid].rec_port))

        sock.sendall(json.dumps(dict).encode('utf-8'))
        sock.close()


    def send_message_for_group(self):
        """
        Grouping happens after all workers are done mapping. This function will
        evenly split the files in tmp/job-X/mapper-output/ amongst all workers.
        The message to send to the workers is:
        {
          "message_type": "new_sort_job",
          "input_files": [list of strings],
          "output_file": string,
          "worker_pid": int
        }
        Ray
        :return:
        """

        # take in all mapper-output files into a list
        mapped_output_path = "tmp/job-0/mapper-output/"
        mapped_filenames = [f for f in os.listdir(mapped_output_path)]

        # create dictionary containing ready workers and message to send
        template_message = {"message_type": "new_sort_job",
                            "input_files": [],
                            "output_file": "grouped",
                            "worker_pid": -1}
        messages_to_send = {}

        for worker in self.workers:
            if worker.ready and not worker.dead:
                messages_to_send[worker.id] = template_message.copy()
                messages_to_send[worker.id]["worker_pid"] = worker.id
                messages_to_send[worker.id]["output_file"] += str(
                    worker.id)

        files_per_worker = int(len(mapped_filenames) / len(messages_to_send))

        # evenly assign filenames amongst ready workers
        c = 0
        for wid in messages_to_send:
            messages_to_send[wid]["input_files"] = mapped_filenames[
                                                c:c + files_per_worker]
            c += files_per_worker

        for wid in messages_to_send:
            if c >= len(mapped_filenames):
                break
            messages_to_send[wid]["input_files"].append(mapped_filenames[c])
            c += 1

        # send all messages to workers
        for wid in messages_to_send:
            self.send_tcp_message(messages_to_send[wid])


if __name__ == "__main__":
    m = Master(8000)