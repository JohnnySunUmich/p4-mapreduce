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
        # self.map_task_id = 0
        self.map_tasks = Queue() #available map task queue
        # self.reduce_task_id = 0
        self.reduce_tasks = Queue() #available reduce task queue
        #self.num_remaining_map_tasks = 0
        #self.num_remaining_reduce_tasks = 0
        #create a dictionary for each worker's last time sending heartbeat:
        #use (host, port) as key
        self.lastBeat = {}
        self.manager_state = "ready"
        self.taskState = "begin" #track if it si tasking state or reducing state
        self.receiveCount = 0 #track the finished map job
        self.finishCount = 0 #track the finished reduce job
        self.job_queue = Queue() #for pending jobs to be exevute 
        self.currentJob = {} #for reassign
        self.tempDir = "" #for reassign
        #start running the main thing : 
        #for three things be at the same time : 
        # listen TCP/ job running/ listen UDP / check dead
        
        job_running_thread  = threading.Thread(target=self.check_job_queue)
        job_running_thread.start()
        heartbeat_thread = threading.Thread(target=self.listen_hb)
        heartbeat_thread.start()
        check_dead_thread = threading.Thread(target=self.check_dead)
        check_dead_thread.start()
        #the main thread for listening for message:
        self.listen_messages()
        #self.shutdown = True
        
        #main_thread  = threading.Thread(target=self.listen_messages)
        #main_thread.start()
        #listen_new_message  = threading.Thread(target=self.listen_new_messages)
        #listen_new_message.start()
        
        #when shutdown is that need to wait for all threads to complete or terminate all?
        #listen_new_message.join()
        heartbeat_thread.join()
        check_dead_thread.join()
        job_running_thread.join()
        #main_thread.join()

    
    #create an inner class of Worker:
    class Worker:
        def __init__(self, worker_host, worker_port, state, task) :
            self.worker_host = worker_host
            self.worker_port = worker_port
            self.state = state
            self.current_task = task

    #a function for listening to non-heartbeat incoming messages :
    def listen_messages(self) :
        #use TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port)) 
            sock.listen()
            sock.settimeout(1)
            print("Created server socket")
            #handle things here that while not shutting down 
            while self.shutdown is not True:
                # self.check_job_queue()
                # check job_queue
                
                # Wait for a connection for 1s.  The socket library avoids consuming
                # CPU while waiting for a connection.
                print("start listening messages\n")              

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
                print("Received message:")
                print(message_dict)
                #and then determine if a message does something 
                message_type = message_dict["message_type"]
                if message_type == "new_manager_job" :
                    self.handle_job_request(message_dict)
                elif message_type == "register" :
                    self.handle_register(message_dict)
                    # TODO: check succcess?
                elif message_type == "shutdown" :
                    self.handle_shutdown()
                    break
                elif message_type == "finished" :
                    print("received finished message")
                    #first change the worker's state to ready again:
                    pid = self.get_worker_id(message_dict["worker_host"], message_dict["worker_port"])
                    self.update_ready(pid)
                    if self.taskState == "mapping" :
                        print("mapping now")
                        self.receiveCount += 1
                        print("finished map task num:")
                        print(self.receiveCount)
                        print("num_mappers needed:")
                        print(self.currentJob["num_mappers"])
                        if self.receiveCount == self.currentJob["num_mappers"]:
                            self.taskState = "map_finished"
                    elif self.taskState == "reducing" :
                        print("reducing now")
                        self.finishCount += 1
                        print(self.finishCount)
                        print(self.currentJob["num_reducers"])
                        if self.finishCount == self.currentJob["num_reducers"]:
                            self.taskState = "reduce_finished"
                    #use else here for both new manager job and finish
                    #call call the handle job here because in this case the momnet received the last
                    #finished from mapping, we can directly call reducing
                    #the thing of check job queue change to that if queue is not empty and manager free now
                    #call the handle job, create a senario for the new execution
                time.sleep(0.1)
        print("listening messages finished\n")  
    
    
    #a function to handle job request:
    def handle_job_request(self, message_dict):
        print("manager received new job ")
        #first assign a job id
        job_id = self.jobCount
        self.jobCount += 1
        message_dict["job_id"] = job_id
        self.job_queue.put(message_dict)
        print("added new job to job queue")

    def check_job_queue(self):
        while self.shutdown is not True:
            time.sleep(0.1)
            # TODO: make this a new thread?
            print ("starting checking job queue")
            print (self.job_queue.empty())
            print (self.manager_state)
            print (self.get_free_workers())
            if (not self.job_queue.empty()) and self.manager_state == 'ready':
                print ("running a job!")
                self.manager_state = "busy"
                self.taskState == "begin"
                # self.get_free_workers() #TODO: needed here?
                message_dict = self.job_queue.get()
                self.currentJob = message_dict
                
                output_dir = message_dict["output_directory"]
                # delete output directory if exists
                if os.path.exists(output_dir):
                    shutil.rmtree(output_dir)
                    LOGGER.info("deleted output directory %s", output_dir)
                # create output directory
                os.makedirs(output_dir)
                LOGGER.info("Created output directory %s", output_dir)
                #create temp dir need to call both mapping and reducing inside it:
                prefix = f"mapreduce-shared-job{message_dict['job_id']:05d}-"
                with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
                    LOGGER.info("Created tmpdir %s", tmpdir)
                    self.tempDir = tmpdir
                    while (self.taskState == "begin" and self.shutdown == False): 
                        print(self.taskState)
                        self.partition_mapping(message_dict, tmpdir)
                        time.sleep(0.1)
                    while (self.taskState == "mapping" and self.shutdown == False): 
                        print(self.taskState)
                        self.assign_map_work(message_dict, tmpdir)
                        time.sleep(0.1)
                    while (self.taskState == "map_finished" and self.shutdown == False): 
                        print(self.taskState)
                        self.partition_reducing(message_dict, tmpdir)
                        time.sleep(0.1)
                    while (self.taskState == "reducing" and self.shutdown == False): 
                        print(self.taskState)
                        self.assign_reduce_work(message_dict)
                        time.sleep(0.1)
                    # if self.taskState == "reduce_finished":
                    #        self.taskState = "complete"
                LOGGER.info("Cleaned up tmpdir %s", tmpdir)
                self.manager_state = "ready"
        print("check job queue finished")

    #a function to handle registering workers:
    def handle_register(self, dic) :
        print("start handling register")
        #first get the worker host and port
        workerHost = dic["worker_host"]
        workerPort = dic["worker_port"]
        #create a dictionary that stores the worker's info:
        #change the worker list of dic  to a dictionary of key: workerid, value: worker object
        #TODO: another way to handle this, using bool success?
        worker_id = self.workerCount #worker pid for identification
        worker = self.Worker(workerHost, workerPort, "ready", {}) #task is a dict
        self.workers[worker_id] = worker
        self.workerCount += 1
        LOGGER.info('First try to register Worker (%s, %s)', workerHost, workerPort)
        #then send a message back to the worker 
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.connect((workerHost, workerPort))
                message = json.dumps({
                    "message_type" : "register_ack",
                    "worker_host" : workerHost,
                    "worker_port" : workerPort
                })
                sock.sendall(message.encode('utf-8'))
            except ConnectionRefusedError:
                LOGGER.info("ConnectionRefusedError")
                #dead_id = self.get_worker_id(workerHost, workerPort)
                self.mark_worker_dead(worker_id)
        LOGGER.info('Registered Worker (%s, %s)', workerHost, workerPort)
    
    def handle_shutdown(self) :
        for worker in self.workers.values() :
            if worker.state != "dead":
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    try:
                        sock.connect((worker.worker_host, worker.worker_port))
                        message = json.dumps({
                            "message_type" : "shutdown"
                        })
                        sock.sendall(message.encode('utf-8'))
                    except ConnectionRefusedError:
                        LOGGER.info("ConnectionRefusedError")
                worker.state = "dead"
        self.shutdown = True
        print("marked shutdown")

    def get_free_workers(self) :
        have_free_workers = False 
        self.freeWorkers.clear() #first empty the free worker dictionary
        for workerID, worker in self.workers.items() : #when iterate, worker is the key
            if worker.state == "ready" :
                have_free_workers = True
                self.freeWorkers[workerID] = worker
        print("now we have free workers, num:")
        print(len(self.freeWorkers))
        print("now we have total workers, num:")
        print(len(self.workers))
        return have_free_workers

    def sorting(self, input_list, num_workers, numFiles, tasks, task_type) :
        # sort input list
        sorted_input_list = sorted(input_list)
        #create numTasks of lists and add them to the list of tasks
        for i in range(num_workers) :
            if task_type == 'map':
                tasks.append({'task_type': 'map', 'task_id': i, 'task_files': []})
            elif task_type == 'reduce':
                tasks.append({'task_type': 'reduce', 'task_id': i, 'task_files': []})
        #then iterate through all files and assign them to workers
        for i in range(numFiles) :
            #i % numTasks
            tasks[i % num_workers]['task_files'].append(sorted_input_list[i])
        #tasks is now a list of "task_dict"

    def update_busy(self, worker_id) :
        self.workers[worker_id].state = "busy"

    def update_ready(self, worker_id) :
        self.workers[worker_id].state = "ready"
        
    #for mapping:
    def partition_mapping(self, message_dict, tmpdir) :
        #self.map_task_id = 0
        #self.map_tasks = []
        #input_directory = message_dict["input_directory"]
        #input_list = os.listdir(input_directory)
        input_list = []
        for file in os.listdir(message_dict["input_directory"]):
            joined_path = os.path.join(message_dict["input_directory"], file)
            input_list.append(joined_path)
        #input_path = pathlib.Path(message_dict["input_directory"])
        #input_list = []
        #for file in input_path.iterdir():
            #input_list.append(str(file))
        num_needed_mappers = message_dict["num_mappers"]
        #self.num_remaining_map_tasks = num_needed_mappers
        numFiles = len(input_list)

        #sort the files and tasks:
        # TODO: check correctness?
        partitioned_tasks = []
        self.sorting(input_list, num_needed_mappers, numFiles, partitioned_tasks, "map")
        #partitioned tasks is now a list of "task_dict"
        for partitioned_task in partitioned_tasks:
            self.map_tasks.put(partitioned_task)
        print("finished partioning map tasks")
        #now self.map_tasks added num_needed_mappers entries
        self.assign_map_work(message_dict, tmpdir)

    def assign_map_work(self, message_dict, tmpdir) :
        # if finished map, return
        if self.receiveCount == self.currentJob["num_mappers"]:
            return
        # if no available map task, return
        if self.map_tasks.empty():
            return
        print("starting assigning map tasks")
        num_reducers = message_dict["num_reducers"]
        executable = message_dict["mapper_executable"]

        #print("num_remaining_map_tasks:")
        #print(self.num_remaining_map_tasks)

        #tasks will be a list of (list of filename strings)
        #now tasks have num_needed_mappers entries
        while not self.map_tasks.empty() and not self.shutdown and self.taskState == "mapping":
            time.sleep(0.1)
            # update free worker each loop!!!!
            self.get_free_workers()
            print("trying to assigning tasks to mappers")
            for worker_id, worker in self.freeWorkers.items():
                print("free worker num?")
                print(len(self.freeWorkers))
                print("available map task num?")
                print(self.map_tasks.qsize())
            for worker_id, worker in self.freeWorkers.items():
                print(len(self.freeWorkers))
                print(worker_id)
                print(worker.worker_host)
                print(worker.worker_port)
                success = True
                task = self.map_tasks.get()
                print(task)
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    host = worker.worker_host
                    port = worker.worker_port
                    try:
                        sock.connect((host, port))
                        message = json.dumps({
                            "message_type" : "new_map_task",
                            "task_id" : task['task_id'],
                            "input_paths" : task['task_files'], #list of filename strings
                            "executable" : executable,
                            "output_directory" : tmpdir,
                            "num_partitions" : num_reducers,
                            "worker_host" : host,
                            "worker_port" : port
                        })
                        print(message)
                        sock.sendall(message.encode('utf-8'))
                    except ConnectionRefusedError:
                        LOGGER.info("ConnectionRefusedError")
                        success = False
                        #dead_id = self.get_worker_id(host, port)
                        self.mark_worker_dead(worker_id)
                        self.map_tasks.put(task)
                        print("dead")
                if(success):
                    LOGGER.info("send map test")
                    self.update_busy(worker_id) #update worker's state to busy
                    self.workers[worker_id].current_task = task
                    #self.map_task_id += 1

        self.taskState = "mapping"
    
    #for reducing:
    def partition_reducing(self, message_dict, tmpdir) :
        #self.reduce_task_id = 0
        #self.rudece_tasks = []
        #open the tempdir it create and use the filename inside
        #use str(file) to turn the file name just to string

        #temp_path = pathlib.Path(tmpdir)
        allPaths = []
        for file in os.listdir(tmpdir):
            joined_path = os.path.join(tmpdir, file)
            allPaths.append(joined_path)
        #allPaths = []
        #for file in tmpdir.iterdir() :
            #allPaths.append(str(file))

        num_needed_reducers = message_dict["num_reducers"]
        #self.num_remaining_reduce_tasks =  num_needed_reducers
        numFiles = len(allPaths)

        #sort the files and tasks for the workers:
        partitioned_tasks = []
        self.sorting(allPaths, num_needed_reducers, numFiles, partitioned_tasks, "reduce")
        for partitioned_task in partitioned_tasks:
            self.reduce_tasks.put(partitioned_task)
        print("finished partioning reduce tasks")
        #now self.reduce_tasks added num_needed_reducers entries
        self.assign_reduce_work(message_dict)

    def assign_reduce_work(self, message_dict) :
        # if finished reduce, return
        if self.finishCount == self.currentJob["num_reducers"]:
            return
        # if no available reduce task, return
        if self.reduce_tasks.empty():
            return
        print("starting assigning reduce tasks")

        #print("num_remaining_reduce_tasks:")
        #print(self.num_remaining_reduce_tasks)
        #use the files in the tempdir as the input_paths(filename) to pass to workers
        #the manager also creates a temp output
        executable = message_dict["reducer_executable"]
        output_directory = message_dict["output_directory"]
        #tasks will be a list of (list of filename strings)
        
        #send the message to reducers:
        while not self.reduce_tasks.empty() and not self.shutdown and self.taskState == "reducing":
            time.sleep(0.1)
            # update free worker each loop!!!!
            self.get_free_workers()
            print("trying to assigning tasks to reducers")
            for worker_id, worker in self.freeWorkers.items() :
                print("Assigning tasks to reducers")
                success = True
                task = self.reduce_tasks.get()
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    host = worker.worker_host
                    port = worker.worker_port
                    try:
                        sock.connect((host, port))
                        message = json.dumps({
                            "message_type" : "new_reduce_task",
                            "task_id" : task['task_id'],
                            "executable" : executable,
                            "input_paths" : task['task_files'],
                            "output_directory" : output_directory,
                            "worker_host" : host,
                            "worker_port" : port
                        })
                        print(message)
                        sock.sendall(message.encode('utf-8'))
                    except ConnectionRefusedError:
                        LOGGER.info("ConnectionRefusedError")
                        success = False
                        #dead_id = self.get_worker_id(host, port)
                        self.mark_worker_dead(worker_id)
                        self.reduce_tasks.put(task)
                        print("dead")
                if(success):
                    self.update_busy(worker_id) #update worker's state to busy
                    self.workers[worker_id].current_task = task
                    #self.reduce_task_id += 1

        self.taskState = "reducing"

    #a function for listening to heartbeat messages :
    def listen_hb(self) :
        """Listen to workers' heartbeat messages"""
        print("start listening hb")
        #use UDP
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port)) #which ports should be binded 
            sock.settimeout(1)

            while self.shutdown is not True: 
                try:
                    message_bytes = sock.recv(4096)
                except socket.timeout:
                    continue
                message_str = message_bytes.decode("utf-8")
                try:
                    message_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    continue
                if message_dict['message_type'] == 'heartbeat':
                    #looping through workers and if one has not for 10s mark as dead
                    wHost = message_dict["worker_host"]
                    wPort = message_dict["worker_port"]
                    self.lastBeat[(wHost, wPort)] = time.time()
                    LOGGER.info('Received heartbeat from Worker (%s, %s)', wHost, wPort)
                    #still need to create a function to reassign works of dead workers
        print("listening hb finished")
    
    def check_dead(self):
        print("start checking dead")
        while self.shutdown is not True: 
            time.sleep(0.1)
            for workerID, worker in self.workers.items() : #iterate
                wHost = worker.worker_host
                wPort = worker.worker_port
                #print("In total worker:")
                #print((wHost, wPort))
                # TODO: haven't sent any heartbeat?
                if ((wHost, wPort) in self.lastBeat and time.time() - self.lastBeat[(wHost, wPort)] >= 10) :
                    LOGGER.info("Some worker died")
                    self.lastBeat.pop((wHost, wPort), None) #TODO: check
                    LOGGER.info(worker.state)
                    if worker.state == "busy" :
                        LOGGER.info("Dead worker is busy")
                        # hand over task
                        if worker.current_task['task_type'] == "map":
                            self.map_tasks.put(worker.current_task)
                            LOGGER.info("New map task added")
                            #self.num_remaining_map_tasks += 1
                        elif worker.current_task['task_type'] == "reduce":
                            self.reduce_tasks.put(worker.current_task)
                            LOGGER.info("New reduce task added")
                            #self.num_remaining_reduce_tasks += 1
                    # after checking its state, mark it as dead!!!
                    self.mark_worker_dead(workerID)
        print("checking dead finished")
    
    def get_worker_id(self, host, port) :
        for pid in self.workers:
            if self.workers[pid].worker_host == host and self.workers[pid].worker_port == port :
                return pid

    def mark_worker_dead(self, dead_id) :
        self.workers[dead_id].state = "dead"

    #if a worker dies, assign its tasks to the next available worker:
    #TODO: delete this
    def reassign_task(self, dead_id) :
        self.get_free_workers()
        newWorker = list(self.freeWorkers.items())[0]
        #the task file to be re-distributed :
        task_file = self.workers[dead_id].current_task
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((newWorker.worker_host, newWorker.worker_port))
            #the task to be reassigned
            #TODO: check success? delete this?
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
                    "input_paths" : task_file,
                    "executable" : self.currentJob["reducer_executable"],
                    "output_directory" : self.currentJob["output_directory"]
                })
            sock.sendall(message.encode('utf-8'))
        new_id = self.get_worker_id(newWorker.worker_host, newWorker.worker_port)
        self.update_busy(new_id)
        self.workers[new_id].current_task = task_file


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
