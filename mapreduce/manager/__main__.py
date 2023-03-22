"""MapReduce framework Manager node."""
import os
import tempfile
import logging
import socket
import threading
import json
import time
import click
import shutil
from queue import Queue


# Configure logging
LOGGER = logging.getLogger(__name__)

class Manager:
    """Represent a MapReduce framework Manager node."""
    def __init__(self, host, port):
        LOGGER.info("Starting manager")
        """Construct a Manager instance and start listening for messages."""
        self.host = host
        self.port = port
        self.shutdown = False
        #create a list for workers that are registered to it
        self.workers = {} #a dictionary of worker objects
        self.workerCount = 0 #should there be an worker_id in the dictionary for the quick access?
        self.freeWorkers = {} #all the workers that are ready
        self.jobCount = 0 #used for job_id
        #create a dictionary for each worker's last time sending heartbeat:
        #use worker_id as key
        self.lastBeat = {}
        self.manager_state = "ready"
        self.taskState = "" #track if it si tasking state or reducing state
        self.receiveCount = 0 #track the finished map job
        self.job_queue = Queue() #for pending jobs to be exevute 
        self.currentJob = {} #for reassign
        self.tempDir = "" #for reassign
        #start running the main thing : 
        #for three things be at the same time : shutdown/ job running/ heartbeat
        heartbeat_thread = threading.Thread(target=self.listen_hb)
        heartbeat_thread.start()
        #the main thread for listening for message:
        main_thread  = threading.Thread(target=self.listen_messages)
        main_thread.start()
        shutdown_thread = threading.Thread(target=self.listen_messages)
        shutdown_thread.start()
        #when shutdown is that need to wait for all threads to complete or terminate all?
        main_thread.join()
        heartbeat_thread.join()
        shutdown_thread.join()
    
    #create an inner class of Worker:
    class Worker:
        def __init__(self, worker_host, worker_port, state, tasks) :
            self.worker_host = worker_host
            self.worker_port = worker_port
            self.state = state
            self.tasks = tasks

    #a function for listening to non-heartbeat incoming messages :
    def listen_messages(self) :
        print("listening messages")
        #use TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port)) 
            sock.listen()
            sock.settimeout(1)
            print("Created server socket")
            #handle things here that while not shutting down 
            while self.shutdown is not True:
                # check job_queue
                self.check_job_queue()
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
                print(address)
                #and then determine if a message does something 
                message_type = message_dict["message_type"]
                if message_type == "new_manager_job" :
                    self.handle_job_request(message_dict)
                elif message_type == "register" :
                    self.handle_register(message_dict)
                elif message_type == "shutdown" :
                    self.handle_shutdown()
                elif message_type == "finished" :
                    #first change the worker's state to ready again:
                    pid = self.get_worker_id(message_dict["worker_host"], message_dict["worker_port"])
                    self.update_ready(pid)
                    if self.taskState == "mapping" :
                        self.receiveCount += 1

    #a function to handle job request:
    def handle_job_request(self, message_dict):
        print("manager received new job ")
        #first assign a job id
        job_id = self.jobCount
        self.jobCount += 1
        message_dict["job_id"] = job_id
        output_dir = message_dict["output_directory"]
        # delete output directory if exists
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)
            LOGGER.info("deleted output directory %s", output_dir)
        # create output directory
        os.makedirs(output_dir)
        LOGGER.info("Created output directory %s", output_dir)
        self.job_queue.put(message_dict)
        print("added new job to job queue")

    def check_job_queue(self):
        # TODO: make this a new thread?
        print ("starting checking job queue")
        print (self.job_queue.empty())
        print (self.manager_state)
        print (self.get_free_workers())
        if (not self.job_queue.empty()) and self.manager_state == 'ready':
            print ("running a job!")
            self.manager_state = "busy"
            self.get_free_workers() #TODO: needed here?
            message_dict = self.job_queue.get()
            self.currentJob = message_dict
            #create temp dir need to call both mapping and reducing inside it:
            prefix = f"mapreduce-shared-job{message_dict['job_id']:05d}-"
            with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
                LOGGER.info("Created tmpdir %s", tmpdir)
                self.tempDir = tmpdir
                self.partition_mapping(message_dict, tmpdir)
                if self.receiveCount == message_dict["num_mappers"]:
                    self.reducing(message_dict, tmpdir)
            LOGGER.info("Cleaned up tmpdir %s", tmpdir)
            self.manager_state = "ready"

    #a function to handle registering workers:
    def handle_register(self, dic) :
        print("start handling register")
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
        #change the worker list of dic  to a dictionary of key: workerid, value: worker object
        worker_id = self.workerCount #worker pid for identification
        worker = self.Worker(workerHost, workerPort, "ready",[])
        self.workers[worker_id] = worker
        self.workerCount += 1
        LOGGER.info('Registered Worker (%s, %s)', workerHost, workerPort)
    
    def handle_shutdown(self) :
        for worker in self.workers.values() :
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((worker.worker_host, worker.worker_port))
                message = json.dumps({
                    "message_type" : "shutdown"
                })
                sock.sendall(message.encode('utf-8'))
        self.shutdown = True

    def get_free_workers(self) :
        have_free_workers = False 
        self.freeWorkers.clear() #first empty the free worker dictionary
        for workerID, worker in self.workers.items() : #when iterate, worker is the key
            if worker.state == "ready" :
                have_free_workers = True
                self.freeWorkers[workerID] = worker
        return have_free_workers

    def sorting(self, input_list, numTasks, numFiles, tasks) :
        #create numTasks of lists and add them to the list of tasks
        for i in range(numTasks) :
            temp = []
            tasks.append(temp)
        #then iterate through all files and assign them to workers
        for i in range(numFiles) :
            #i % numTasks
            tasks[i % numTasks].append(input_list[i])

    def update_busy(self, worker_id) :
        self.workers[worker_id].state = "busy"

    def update_ready(self, worker_id) :
        self.workers[worker_id].state = "ready"
        
    #for mapping:
    def partition_mapping(self, message_dict, tmpdir) :
        task_id = 0
        self.get_free_workers()
        mappers = {} #a dictionary
        input_directory = message_dict["input_directory"]
        input_list = os.listdir(input_directory)
        numTasks = message_dict["num_mappers"]
        num_reducers = message_dict["num_reducers"]
        executable = message_dict["mapper_executable"]
        numFiles = len(input_list)
        #find available workers in from the free worker queue :
        #break the for loop when the size of mappers == numTasks
        for ID, free in self.freeWorkers.items():
            mappers[ID] = free
            if len(mappers) == numTasks :
                break
        
        tasks = [] #a list of lists
        #sort the files and tasks:
        self.sorting(input_list, numTasks, numFiles, tasks)
        #after calling this sorting function 
        #tasks will be a list of lists
        index = 0 #use for accessing the list of tasks
        for mapper_id, mapper in mappers.items() :
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                host = mapper.worker_host
                port = mapper.worker_port
                sock.connect((host, port))
                self.update_busy(mapper_id) #update worker's state to busy
                self.workers[mapper_id].tasks.append(tasks[index])
                mapper.tasks.append(tasks[index])
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
        self.taskState = "mapping"
        LOGGER.info("send map test")
    
    #for reducing:
    def reducing(self, message_dict, tempdir) :
        task_id = 0
        self.get_free_workers()
        reducers = {} #a dictrionary 
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
        #stop when the size of reducers dic == num_reducers
        for ID, free in self.freeWorkers.items() :
            reducers[ID] = free
            if len(reducers) == num_reducers :
                break
        tasks = [] #a list of lists 
        #sort the files and tasks for the workers:
        self.sorting(allPaths, num_reducers, numFiles, tasks)
        index = 0 #for accessing task list
        #send the message to reducers:
        for reducer_id, reducer in reducers.items():
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                host = reducer.worker_host
                port = reducer.worker_port
                sock.connect((host, port))
                self.update_busy(reducer_id) #update worker's state to busy
                self.workers[reducer_id].tasks.append(tasks[index])
                reducer.tasks.append(tasks[index])
                message = json.dumps({
                    "message_type" : "new_reduce_task",
                    "task_id" : task_id,
                    "executable" : executable,
                    "input_paths" : tasks[index],
                    "output_directory" : output_directory,
                    "worker_host" : host,
                    "worker_port" : port
                })
                sock.sendall(message.encode('utf-8'))
            task_id += 1
            index += 1
        self.taskState = "reducing"

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
                    self.mark_worker_dead(workerID)
                    self.reassign_task(workerID)
                self.lastBeat["workerID"] = time.time()
                #still need to create a function to reassign works of dead workers
    
    def get_worker_id(self, host, port) :
        for pid in self.workers:
            if self.workers[pid].worker_host == host and self.workers[pid].worker_port == port :
                return pid

    def mark_worker_dead(self, dead_id) :
        self.workers[dead_id].state = "dead"

    #if a worker dies, assign its tasks to the next available worker:
    def reassign_task(self, dead_id) :
        self.get_free_workers()
        newWorker = list(self.freeWorkers.items())[0]
        #the task file to be re-distributed :
        task_file = self.workers[dead_id].tasks
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((newWorker.worker_host, newWorker.worker_port))
            #the task to be reassigned
            if self.taskState == "mapping" :
                message = json.dumps({
                    "message_type" : "re_map",
                    "input_paths" : task_file,
                    "executable" : self.currentJob["mapper_executable"],
                    "num_partitions" : self.currentJob["num_reducers"],
                    "output_directory" : self.tempDir
                })
            elif self.taskState == "reducing" :
                message = json.dumps({
                    "message_type" : "re_reduce",
                    "input_path" : task_file,
                    "executable" : self.currentJob["reducer_executable"],
                    "output_directory" : self.currentJob["output_directory"]
                })
            sock.sendall(message.encode('utf-8'))
        new_id = self.get_worker_id(newWorker.worker_host, newWorker.worker_port)
        self.update_busy(new_id)
        self.workers[new_id].task = task_file


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
