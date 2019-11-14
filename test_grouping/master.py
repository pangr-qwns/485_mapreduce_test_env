import os
import threading
import socket
from pprint import pprint
from worker import Worker
import json
import heapq
import contextlib
import copy

class Master:
    def __init__(self, port_num):
        w = Worker(0, 6000, 6001)
        w2 = Worker(1, 6002, 6003)
        wdead = Worker(2, 6004, 6005)
        wdead.dead = True
        self.workers = [w, w2, wdead]
        self.split_merge_into_reduces()


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


    def grouping_stage(self):
        """
        After all workers group their files into one larger file, the master
        will merge the files in tmp/job-X/grouper-output/ into one file. Then
        the master will partition that file into the correct number of files
        or the reducers.
        Ray
        :return:
        """
        self.merge_groupings()
        self.split_merge_into_reduces()


    def merge_groupings(self):
        grouped_path = "tmp/job-0/grouper-output/"
        grouped_filenames = [os.path.join(grouped_path, f) for f in
                             os.listdir(grouped_path)]
        # use heapq's merge
        output = "tmp/job-0/grouper-output/merge.txt"
        # Citation: https://stackoverflow.com/questions/26240228/
        with contextlib.ExitStack() as stack:
            files = [stack.enter_context(open(fn)) for fn in grouped_filenames]
            with open(output, 'w') as f:
                f.writelines(heapq.merge(*files))


    def split_merge_into_reduces(self):
        """Splits merge.txt into num_reducers number of chunks. Each chunk
        has a distinct key."""

        num_reducers = 5
        message_template = {
            "message_type": "new_worker_job",
            "input_files": ["tmp/job-0/grouper-output/"],
            "executable": "reducer_executable",
            "output_directory": "tmp/job-0/reducer-output/",
            "worker_pid": -1
        }
        reducer_msgs = [copy.deepcopy(message_template) for _ in range(num_reducers)]
        for i in range(len(reducer_msgs)):
            reducer_msgs[i]["input_files"][0] += "reduce%i.txt" % (i+1)
            reducer_msgs[i]["output_directory"] += "reduce%i.txt" % (i+1)

        merge_path = "tmp/job-0/grouper-output/merge.txt"
        count = 0
        prev = 'a'
        with open(merge_path) as merge_file:
            for line in enumerate(merge_file):
                # line is a pair
                # index 0 is the line number
                # index 1 is the text.
                # line[1][-2] is the value as a str
                # line[1][0] is the key as a str
                key = line[1][0]
                if line[0] == '0':
                    prev = key
                if key == prev:
                    # keep writing to count
                    with open(reducer_msgs[count]["input_files"][0], "a+") as f:
                        f.write(line[1])
                elif key != prev:
                    # new key
                    count = (count + 1) % 5
                    with open(reducer_msgs[count]["input_files"][0], "a+") as f:
                        f.write(line[1])
                    prev = key

        worker_ind = 0
        for i in range(len(reducer_msgs)):
            if not self.workers[worker_ind].dead:
                print(i)
                print(worker_ind)
                reducer_msgs[i]["worker_pid"] = worker_ind
                self.send_tcp_message(reducer_msgs[i])
            worker_ind = (worker_ind + 1) % len(self.workers)



if __name__ == "__main__":
    m = Master(8000)