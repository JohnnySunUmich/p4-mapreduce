"""MapReduce framework Worker node."""
import os
import logging
import socket
import json
import time
import threading
import click
import shutil
import hashlib
import tempfile
import subprocess
import heapq
from queue import Queue

# Configure logging
LOGGER = logging.getLogger(__name__)

class Worker:
    """A class representing a Worker node in a MapReduce cluster."""
    def __init__(self, host, port, manager_host, manager_port): 
        """Construct a Worker instance and start listening for messages."""
        self.tasks = Queue()
        self.state = ""
        self.registered = False
        self.host = host
        self.port = port
        self.manager_host = manager_host
        self.manager_port = manager_port
        self.shutdown = False
        #thread for sending heartbeat message:
        send_hb_thread = threading.Thread(target=self.send_heartbeat)
        send_hb_thread.start()
        self.listen()
        send_hb_thread.join()

    #a function to listen to the manager's tcp message :
    def listen(self) :
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            sock.listen()
            sock.settimeout(1)
            while self.shutdown == False:
                #first register it if not already done so:
                if self.registered == False :
                    self.register()
                #then get the message :
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    continue
                
                clientsocket.settimeout(1)
                with clientsocket:
                    message_chunks = []
                    while True:
                        try:
                            data = clientsocket.recv(4096)
                        except socket.timeout:
                            continue
                        if not data:
                            break
                        message_chunks.append(data)
                message_bytes = b''.join(message_chunks)
                message_str = message_bytes.decode("utf-8")
                try:
                    message_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    continue
                #now determine what kind of message it is :
                message_type = message_dict["message_type"]
                if message_type == "shutdown" :
                    self.shutdown = True
                elif message_type == "register_ack" :
                    print("register acknowledged")
                    self.registered = True
                    self.state = "ready"
                elif message_type == "new_map_task" or message_type == "re_map":
                    self.mapping(message_dict)
                elif message_type == "new_reduce_task" or message_type == "re_reduce":
                    self.reducing(message_dict)
    
            
    #a function that sends register:
    def register(self) :
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.manager_host, self.manager_port))
            message = json.dumps({
                "message_type" : "register",
                "worker_host" : self.host,
                "worker_port" : self.port
            })
            sock.sendall(message.encode('utf-8'))
            print("sent register message")
    
    #send heartbeat message:
    def send_heartbeat(self) :
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect((self.manager_host, self.manager_port))
            message = json.dumps({
                "message_type" : "heartbeat",
                "worker_host" : self.host,
                "worker_port" : self.port
            })
            sock.sendall(message.encode('utf-8'))
            time.sleep(2)
    
    #for performing mapping tasks:
    def mapping(self, message_dict) :
        executable = message_dict["executable"]
        input_paths = message_dict["input_paths"]
        output_dir = message_dict["output_directory"]
        num_partitions = message_dict["num_partitions"]

        #run the executable :
        prefix = f"mapreduce-local-task{message_dict['task_id']:05d}-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            for input_path in input_paths :
                with open(input_path) as infile:
                    with subprocess.Popen(
                        executable,
                        stdin=infile,
                        stdout=subprocess.PIPE,
                        text=True,
                    ) as map_process :
                        for line in map_process.stdout :
                            line = line.strip()
                            if not line:
                                continue
                            key = line.split("\t")[0]
                            hexdigest = hashlib.md5(key.encode("utf-8")).hexdigest()
                            keyhash = int(hexdigest, base=16)
                            partition_number = keyhash % num_partitions
                            part_file_name = f"maptask{message_dict['task_id']:05d}-part{f'{partition_number:05d}'}"
                            part_file_path = os.path.join(tmpdir, part_file_name)
                            with open(part_file_path, 'a+', encoding="utf-8") as part_file:
                                part_file.write(line)
                                part_file.close()
                            
            #after this the worker open the directory :
            os.chdir(tmpdir)
            files = os.listdir()
            #then open each file to sort the values in each file and write back
            for file in files :
                # TODO: check correctness
                with open(file, 'r') as currFile :
                    tempList = []
                    contents = currFile.read()
                    for content in contents :
                        tempList.append(content)
                    tempList = sorted(tempList)
                    with open(file, 'w') as currFile :
                        currFile.writelines(tempList)
                        currFile.close()
                # move sorted temp files to output dir.
                shutil.move(os.path.join(tmpdir, file),
                            os.path.join(output_dir, file))

        #now the worker sends back the finished message to the manager :
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.manager_host, self.manager_port))
            message = json.dumps({
                "message_type" : "finished",
                "task_id" : message_dict["task_id"],
                "worker_host" : self.host,
                "worker_port" : self.port
            })
            sock.sendall(message.encode('utf-8'))
    
    #a functin for the reduce
    def reducing(self, message_dict) :
        executable = message_dict["executable"]
        output_directory = message_dict["output_directory"]
        #merge input files into one sorted output stream
        instream = heapq.merge(message_dict["inpupt_paths"])
        #need to make the task_id a 5 digit number: 
        task_id = '{:05d}'.format(message_dict["task_id"])

        #now deal with the outfile : 
        with tempfile.TemporaryDirectory(prefix='mapreduce-local-task{}-'.format(task_id)) as tmp_dir2: 
            outfile = '{}/part-{}'.format(tmp_dir2, task_id)
            with subprocess.Popen(
            executable,
            text=True,
            stdin=subprocess.PIPE,
            stdout=outfile,
            ) as reduce_process :
                for line in instream:
                    reduce_process.stdin.write(line)
        
        #now move the output file to the output directory:
        shutil.move(outfile, output_directory)
        #then send the finish message back to the manager:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
             sock.connect((self.manager_host, self.manager_port))
             message = json.dumps({
                "message_type" : "finished",
                "task_id" : message_dict["task_id"],
                "worker_host" : self.host,
                "worker_port" : self.port
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
