"""MapReduce framework Manager node."""
import os
import tempfile
import logging
import socket
import threading
import json
import time
import click
import mapreduce.utils
import shutil
from pathlib import Path
from queue import Queue
import heapq

# Configure logging
LOGGER = logging.getLogger(__name__)

class Manager:
    """Represent a MapReduce framework Manager node."""
    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""
        self.host = host
        self.port = port
        self.shutdown = False
        #create a list for workers that are registered to it
        self.workers = [] #an array of dictionary 
        self.workerCount = 0 #should there be an worker_id in the dictionary for the quick access?
        self.freeWorkers = Queue() #all the workers that are ready
        self.jobCount = 0 #used for job_id
        #create a dictionary for each worker's last time sending heartbeat:
        #use worker_id as key:
        self.lastBeat = {}
        #later have to use thread
        #for three things be at the same time : shutdown/ job running/ heartbeat

    #a function for listening to non-heartbeat incoming messages :
    def listen_messages(self) :
        #use TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port)) 
            sock.listen()
            sock.settimeout(1)
            #handle things here that while not shutting down 
            while self.shutdown is not True:
                # Wait for a connection for 1s.  The socket library avoids consuming
                # CPU while waiting for a connection.
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    continue
                # Socket recv() will block for a maximum of 1 second.  If you omit
                # this, it blocks indefinitely, waiting for packets.
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

                # Decode list-of-byte-strings to UTF8 and parse JSON data
                message_bytes = b''.join(message_chunks)
                message_str = message_bytes.decode("utf-8")

                try:
                    message_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    continue
                #and then determine if a message does something 
                message_type = message_dict["message_type"]
                if message_type == "new_manager_job" :
                    self.handle_job_request(message_dict)
                elif message_type == "register" :
                    self.handle_register(message_dict)
                elif message_type == "shutdown" :
                    self.handle_shutdown()

    #a function to handle registering workers:
    def handle_register(self, dic) :
        #first get the worker host and port
        workerHost = dic["worker_host"]
        workerPort = dic["worker_port"]
        #then send a message back to the worker 
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((workerHost, workerPort))
            message = json.dumps({
                "message_type" : "register_ack",
                "worker_host" : workerHost,
                "worker_port" : workerPort
            })
            sock.sendall(message.encode('utf-8'))
            #create a dictionary that stores the worker's info:
            worker = {
                "worker_id" : self.workerCount, #only use for accessing worker's heatbeat time in dictioinary
                "worker_host" : workerHost,
                "worker_port" : workerPort,
                "state" : "ready"
            }
            self.workers.append(worker)
            self.workerCount += 1
        
    
    def handle_shutdown(self) :
        for worker in self.workers :
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((worker["worker_host"], worker["worker_port"]))
                message = json.dumps({
                    "message_type" : "shutdown"
                })
                sock.sendall(message.encode('utf-8'))
        self.shutdown = True

    #a function to handle job request:
    def handle_job_request(self, message_dict):
        #first assign a job id
        self.jobCount += 1
        #create temp dir need to call both mapping and reducing inside it:
        prefix = f"mapreduce-shared-job{self.job_id:05d}-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            self.partition_mapping(message_dict, tempdir)
            #how to determine when to process to the reducing stage?
            #how to know if all the workers sent receive message
            self.reducing(message_dict, tempdir)

    
    def get_free_workers(self) :
        self.freeWorkers.queue.clear()
        for worker in self.workers :
            if worker["state"] is "ready" :
                self.freeWorkers.put(worker)

    def sorting(self, input_list, numTasks, numFiles, tasks) :
        #create numTasks of lists and add them to the list of tasks
        for i in range(numTask) :
            temp = []
            tasks.append(temp)
        #then iterate through all files and assign them to workers
        for i in range(numFiles) :
            #i % numTasks
            tasks[i % numTasks].append(input_list[i])

    def update_busy(self, worker_host, worker_port) :
        for worker in self.workers :
            if worker["worker_host"] is worker_host and worker["worker_port"] is worker_port :
                worker["state"] = "busy"

    #for mapping:
    def partition_mapping(self, message_dict, tempdir) :
        task_id = 0
        self.get_free_workers()
        mappers = Queue()
        input_directory = message_dict["input_directory"]
        input_list = os.listdir(input_directory)
        numTasks = message_dict["num_mappers"]
        num_reducers = message_dict["num_reducers"]
        executable = message_dict["mapper_executable"]
        output_directory = message_dict["output_directory"]
        numFiles = len(input_list)
        #find available workers in from the free worker queue :
        for i in range(numTasks) :
            curr = self.freeWorkers.get()
            mappers.put(curr)
        
        tasks = [] #a list of lists
        #sort the files and tasks:
        self.sorting(input_list, numTasks, numFiles, tasks)
        #after calling this sorting function 
        #tasks will be a list of lists
        index = 0 #use for accessing the list of tasks
        for mapper in mappers :
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                host = mapper["worker_host"]
                port = mapper["worker_port"]
                sock.connect((host, port))
                self.update_busy(host, port) #update worker's state to busy
                message = json.dumps({
                    "message_type" : "new_map_task",
                    "task_id" : task_id,
                    "input_path" : tasks[index], #list of strings/filenames
                    "executable" : executable,
                    "output_directory" : tmpdir,
                    "num_partitions" : num_reducers,
                    "worker_host" : host,
                    "worker_port" : port
                })
                sock.sendall(message.encode('utf-8'))
                task_id += 1
                index += 1
    
    #for reducing:
    def reducing(self, message_dict, tempdir) :
        task_id = 0
        self.get_free_workers()
        reducers = Queue()
        #use the files in the tempdir as the input_paths(filename) to pass to workers
        #the manager also creates a temp output
        executable = message_dict["reducer_executable"]
        num_reducers = message_dict["num_reducers"]
        output_directory = message_dict["output_directory"]
        #open the tempdir it create and use the filename inside
        #use str(file) to turn the file name just to string
        allPaths = []
        files = os.listdir(tempdir)
        for file in files :
            allPaths.append(str(file))
        numFiles = len(allPaths)
        #do the partition job all over again here :
        #first find the workers to be reducers:
        for i in range(num_reducers) :
            reducers.put(self.freeWorkers.get())
        tasks = [] #a list of lists 
        #sort the files and tasks for the workers:
        self.sorting(allPaths, num_reducers, numFiles, tasks)
        index = 0 #for accessing task list
        #send the message to reducers:
        for reducer in reducers :
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                host = reducer["worker_host"]
                port = reducer["worker_port"]
                sock.connect((host, port))
                self.update_busy(host, port) #update worker's state to busy
                message = json.dumps({
                    "message_type" : "new_reduce_task",
                    "task_id" : task_id,
                    "executable" : executable,
                    "output_directory" : message_dict["output_directory"],
                    "worker_host" : host,
                    "worker_port" : port
                })
            sock.sendall(message.encode('utf-8'))
            task_id += 1
            index += 1

    #a function for listening to heartbeat messages :
    def listen_hb(self) :
        """Listen to workers' heartbeat messages"""
        #use UDP
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port)) #which ports should be binded 
            sock.settimeout(1)

            while self.shutdown is False: 
                try:
                    message_bytes = sock.recv(4096)
                except socket.timeout:
                    continue
                message_str = message_bytes.decode("utf-8")
                message_dict = json.loads(message_str)
                #looping through workers and if one has not for 10s mark as dead
                wHost = message_dict["worker_host"]
                wPort = message_dict["worker_port"]
                workerID = self.get_worker_id(wHost, wPort)
                if (workerID in self.lastBeat and time.time() - self.lastBeat["workerID"] >= 10) :
                    mark_worker_dead(wHost, wPort)
                self.lastBeat["workerID"] = time.time()
                #still need to create a function to reassign works of dead workers

    #for heartbeat check: need to get id:
    def get_worker_id(self, host, port) :
        for worker in self.workers :
            if worker["worker_host"] == host and worker["worker_port"] == port :
                return worker["worker_id"]

    def mark_worker_dead(self, dead_worker_host, dead_worker_port) :
        for worker in self.workers :
            if worker["worker_host"] is dead_worker_host and worker["worker_port"] is dead_worker_port :
                worker["state"] = "dead"
    

@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
@click.option("--shared_dir", "shared_dir", default=None)
def main(host, port, logfile, loglevel, shared_dir):
    """Run Manager."""
    tempfile.tempdir = shared_dir
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(
        f"Manager:{port} [%(levelname)s] %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Manager(host, port)


if __name__ == "__main__":
    main()
