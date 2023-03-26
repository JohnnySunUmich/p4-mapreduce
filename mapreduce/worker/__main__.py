"""MapReduce framework Worker node."""
import os
import logging
import socket
import json
import time
import threading
from contextlib import ExitStack
import shutil
import hashlib
import tempfile
import subprocess
import heapq
import click
from mapreduce.utils.util import get_message_str, create_socket

# Configure logging
LOGGER = logging.getLogger(__name__)


class Worker:
    """A class representing a Worker node in a MapReduce cluster."""

    def __init__(self, host, port, manager_host, manager_port):
        """Construct a Worker instance and start listening for messages."""
        self.registered = False
        self.host = host
        self.port = port
        self.manager_host = manager_host
        self.manager_port = manager_port
        self.shutdown = False
        self.threads = []
        self.listen()
        for thread in self.threads:
            thread.join()

    # a function to listen to the manager's tcp message:
    def listen(self):
        """Listen to manager tcp."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            create_socket(self.host, self.port, sock)
            while self.shutdown is False:
                # first register it if not already done so:
                if self.registered is False:
                    self.register()
                # then get the message:
                try:
                    clientsocket1, _ = sock.accept()
                except socket.timeout:
                    continue

                clientsocket1.settimeout(1)
                message_str1 = get_message_str(socket, clientsocket1)
                try:
                    message_dict = json.loads(message_str1)
                except json.JSONDecodeError:
                    continue

                message_type = message_dict["message_type"]
                if message_type == "shutdown":
                    self.shutdown = True
                    break
                if message_type == "register_ack":
                    print("register acknowledged")
                    self.registered = True
                    send_hb_thread = threading.Thread(
                        target=self.send_heartbeat)
                    self.threads.append(send_hb_thread)
                    send_hb_thread.start()
                elif message_type in ('new_map_task', 're_map'):
                    LOGGER.info("Received task MapTask %s",
                                message_dict['task_id'])
                    self.mapping(message_dict)
                elif message_type in ('new_reduce_task', 're_reduce'):
                    LOGGER.info("Received task ReduceTask %s",
                                message_dict['task_id'])
                    self.reducing(message_dict)

    # a function that sends register:
    def register(self):
        """Register."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.manager_host, self.manager_port))
            message = json.dumps({
                "message_type": "register",
                "worker_host": self.host,
                "worker_port": self.port
            })
            sock.sendall(message.encode('utf-8'))
            print("sent register message")

    # send heartbeat message:
    def send_heartbeat(self):
        """Send heartbeat."""
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect((self.manager_host, self.manager_port))
            message = json.dumps({
                "message_type": "heartbeat",
                "worker_host": self.host,
                "worker_port": self.port
            })
            while self.shutdown is not True:
                sock.sendall(message.encode('utf-8'))
                print("heartbeat sent")
                time.sleep(2)

    # for performing mapping tasks:
    def mapping(self, message_dict):
        """Map."""
        # run the executable:
        with tempfile.TemporaryDirectory(
            prefix=f"mapreduce-local-task{message_dict['task_id']:05d}-")\
                as tmpdir:
            with ExitStack() as stack:
                # use stack to reduce time and memory by reducing open times!!!
                files_opened = []
                for input_path in message_dict["input_paths"]:
                    files_opened.append(stack.enter_context(open(input_path,
                                        encoding="utf-8")))
                part_files = []
                for part_num in range(message_dict["num_partitions"]):
                    part_file_name = (f"maptask{message_dict['task_id']:05d}"
                                      f"-part{f'{part_num:05d}'}")
                    part_files.append(
                        stack.enter_context(
                            open(os.path.join(tmpdir, part_file_name),
                                 "a+", encoding="utf-8")
                        )
                    )

                for infile in files_opened:
                    with subprocess.Popen(
                        [message_dict["executable"]],
                        stdin=infile,
                        stdout=subprocess.PIPE,
                        text=True,
                    ) as map_process:
                        for line in map_process.stdout:
                            key = line.split("\t")[0]
                            hexdigest = hashlib.md5(
                                key.encode("utf-8")).hexdigest()
                            partition_number = int(hexdigest, base=16) %\
                                message_dict["num_partitions"]
                            part_files[partition_number].write(line)
            self.sort_and_write(message_dict, tmpdir)

    def sort_and_write(self, message_dict, tmpdir):
        """Sort and write."""
        # open each file to sort the values in each file and write back
        for file in os.listdir(tmpdir):
            # TODOO: check correctness
            file_path = os.path.join(tmpdir, file)
            # open once to reduce time and memory!!!
            with open(file_path, 'r+', encoding="utf-8") as curr_file:
                temp_list = curr_file.readlines()
                temp_list.sort()
                # write from top!!!!!!!
                curr_file.seek(0)
                curr_file.writelines(temp_list)
            # move sorted temp files to output dir.
            shutil.move(os.path.join(tmpdir, file),
                        os.path.join(message_dict["output_directory"], file))

        # now the worker sends back the finished message to the manager:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.manager_host, self.manager_port))
            message = json.dumps({
                "message_type": "finished",
                "task_id": message_dict["task_id"],
                "worker_host": self.host,
                "worker_port": self.port
            })
            sock.sendall(message.encode('utf-8'))

    # a functin for the reduce
    def reducing(self, message_dict):
        """Reduce."""
        print("worker start reducing")
        # need to make the task_id a 5 digit number:
        # task_id = '{:05d}'.format(message_dict["task_id"])
        # now deal with the outfile:
        # prefix = 'mapreduce-local-task{}-'.format(task_id)
        prefix = f"mapreduce-local-task{message_dict['task_id']:05d}-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmp_dir2:
            # merge input files into one sorted output stream
            with ExitStack() as stack:
                files_opened = []
                for file in message_dict["input_paths"]:
                    files_opened.append(stack.enter_context(open(file,
                                        encoding="utf-8")))
                instream = heapq.merge(*files_opened)
                outfile_path = os.path.join(
                    tmp_dir2, f"part-{message_dict['task_id']:05d}")
                outfile = stack.enter_context(
                    open(outfile_path, "w+", encoding="utf-8"))
                with subprocess.Popen(
                    [message_dict["executable"]],
                    text=True,
                    stdin=subprocess.PIPE,
                    stdout=outfile,
                ) as reduce_process:
                    for line in instream:
                        # print(line)
                        reduce_process.stdin.write(line)
                # now move the output file to the output directory:
                shutil.move(outfile_path, message_dict["output_directory"])
        print("worker finish reducing")

        # then send the finish message back to the manager:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.manager_host, self.manager_port))
            message = json.dumps({
                "message_type": "finished",
                "task_id": message_dict["task_id"],
                "worker_host": self.host,
                "worker_port": self.port
            })
            sock.sendall(message.encode('utf-8'))


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6001)
@click.option("--manager-host", "manager_host", default="localhost")
@click.option("--manager-port", "manager_port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
def main(host, port, manager_host, manager_port, logfile, loglevel):
    """Run Worker."""
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(f"Worker:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Worker(host, port, manager_host, manager_port)


if __name__ == "__main__":
    main()
