"""MapReduce framework Manager node."""
import tempfile
import logging
import socket
import threading
import json
from time import time, sleep
from collections import deque
import shutil
from pathlib import Path
from queue import Queue
from types import SimpleNamespace
import click
from mapreduce.utils.util import get_message_str, create_socket


# Configure logging
LOGGER = logging.getLogger(__name__)


class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""
        LOGGER.info("Starting manager")
        self.manager_info = {
            "host": host,
            "port": port,
            "shutdown": False,
            "manager_state": 'ready'
        }

        # TODOO: worker Sate written by multiple threads: race condition?
        self.workers_info = {
            "workers": {},
            "worker_count": 0,
            "free_workers": {},
            "last_beat": {}  # use (host, port) as key
        }

        self.task_info = {
            "task_state": "begin",
            "map_tasks": deque(),  # available map task
            "reduce_tasks": deque(),  # available reduce task queue
            "receive_count": 0,
            "finish_count": 0
        }
        # TODOO: taskSate written by multiple threads: race condition?

        self.job_count = 0  # used for job_id
        self.job_queue = Queue()  # for pending jobs to be exevute
        self.current_job = {}  # for reassign

        self.worker_state_lock = threading.Lock()
        # start running the main thing:
        # for three things be at the same time:
        # listen TCP/ job running/ listen UDP / check dead

        job_running_thread = threading.Thread(target=self.check_job_queue)
        job_running_thread.start()
        heartbeat_thread = threading.Thread(target=self.listen_hb)
        heartbeat_thread.start()
        check_dead_thread = threading.Thread(target=self.check_dead)
        check_dead_thread.start()
        # the main thread for listening for message:
        self.listen_messages()

        # when shutdown: wait for all threads to complete or terminate all?
        heartbeat_thread.join()
        check_dead_thread.join()
        job_running_thread.join()

    # a function for listening to non-heartbeat incoming messages:
    def listen_messages(self):
        """Listen messages on main thread using TCP."""
        # use TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            create_socket(self.manager_info[
                "host"], self.manager_info["port"], sock)
            # handle things here that while not shutting down
            while self.manager_info["shutdown"] is not True:
                # Wait 1s connection for 1s. socket avoids consuming
                # CPU while waiting for a connection.
                print("start listening messages\n")

                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    continue
                # socket.recv() will block for a 1s max.  If you omit
                # this, it blocks indefinitely, waiting for packets.
                clientsocket.settimeout(1)
                message_str = get_message_str(socket, clientsocket)

                try:
                    message_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    continue
                print(address)
                print("Received message:")
                print(message_dict)

                message_type = message_dict["message_type"]
                if message_type == "new_manager_job":
                    self.handle_job_request(message_dict)
                elif message_type == "register":
                    self.handle_register(message_dict)
                elif message_type == "shutdown":
                    self.handle_shutdown()
                    break
                elif message_type == "finished":
                    self.handle_finished(message_dict)
                sleep(0.1)
        print("listening messages finished\n")

    # a function to handle job request:
    def handle_job_request(self, message_dict):
        """Handle job request."""
        print("manager received new job ")
        # first assign a job id
        job_id = self.job_count
        self.job_count += 1
        message_dict["job_id"] = job_id
        self.job_queue.put(message_dict)
        print("added new job to job queue")

    def check_job_queue(self):
        """Check job queue."""
        while self.manager_info["shutdown"] is not True:
            sleep(0.1)
            print("starting checking job queue")
            if self.manager_info["manager_state"] == 'ready':
                print("we are not running a job")
            if (not self.job_queue.empty()) and self.manager_info[
                    "manager_state"] == 'ready':
                print("running a job!")
                self.manager_info["manager_state"] = "busy"
                self.task_info["task_state"] = "begin"
                message_dict = self.job_queue.get()
                self.current_job = message_dict

                output_dir = Path(message_dict["output_directory"])
                # delete output directory if exists
                if output_dir.exists():
                    shutil.rmtree(output_dir)
                    LOGGER.info("deleted output directory %s", output_dir)
                # create output directory
                output_dir.mkdir(parents=True, exist_ok=False)
                LOGGER.info("Created output directory %s", output_dir)
                # create tempdir need call both mapping & reducing inside it:
                prefix = f"mapreduce-shared-job{message_dict['job_id']:05d}-"
                with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
                    LOGGER.info("Created tmpdir %s", tmpdir)
                    while self.task_info[
                            "task_state"] == "begin" and\
                            not self.manager_info["shutdown"]:
                        print(self.task_info["task_state"])
                        self.partition_mapping(message_dict)
                        sleep(0.1)
                    while self.task_info[
                            "task_state"] == "mapping" and\
                            not self.manager_info["shutdown"]:
                        print(self.task_info["task_state"])
                        self.assign_map_work(message_dict, tmpdir)
                        sleep(0.1)
                    print("we are out map loop!!")
                    while self.task_info["task_state"] == "map_finished" and\
                            not self.manager_info["shutdown"]:
                        print(self.task_info["task_state"])
                        self.partition_reducing(message_dict, tmpdir)
                        sleep(0.1)
                    while self.task_info[
                            "task_state"] == "reducing" and\
                            not self.manager_info["shutdown"]:
                        print(self.task_info["task_state"])
                        self.assign_reduce_work(message_dict)
                        sleep(0.1)
                LOGGER.info("Cleaned up tmpdir %s", tmpdir)
                print("we finished a job!")
                self.manager_info["manager_state"] = "ready"
        print("check job queue finished")

    # a function to handle registering workers:
    def handle_register(self, dic):
        """Handle register worker."""
        print("start handling register")
        # first get the worker host and port
        worker_host = dic["worker_host"]
        worker_port = dic["worker_port"]
        # create a dictionary that stores the worker's info:
        # change worker list of dict to a dict: (workerid, value): worker obj
        success = True
        # then send a message back to the worker
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.connect((worker_host, worker_port))
                message = json.dumps({
                    "message_type": "register_ack",
                    "worker_host": worker_host,
                    "worker_port": worker_port
                })
                sock.sendall(message.encode('utf-8'))
            except ConnectionRefusedError:
                success = False
                LOGGER.info("ConnectionRefusedError")
        if success:
            if (worker_host, worker_port) not in self.workers_info[
                    "last_beat"]:
                self.workers_info["last_beat"][
                    (worker_host, worker_port)] = time()
                print("set last beat before registering")
            worker_id = self.workers_info["worker_count"]
            # TODOO: consider revive?
            worker_pod = SimpleNamespace(
                worker_host=worker_host, worker_port=worker_port,
                state="ready", current_task={})
            worker = worker_pod
            self.workers_info["workers"][worker_id] = worker
            self.workers_info["worker_count"] += 1
        LOGGER.info('Registered Worker (%s, %s)', worker_host, worker_port)

    def handle_shutdown(self):
        """Handle shutdown."""
        for worker in self.workers_info["workers"].values():
            if worker.state != "dead":
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    try:
                        sock.connect((worker.worker_host, worker.worker_port))
                        message = json.dumps({
                            "message_type": "shutdown"
                        })
                        sock.sendall(message.encode('utf-8'))
                    except ConnectionRefusedError:
                        LOGGER.info("ConnectionRefusedError")
                worker.state = "dead"
        self.manager_info["shutdown"] = True
        print("marked shutdown")

    def handle_finished(self, message_dict):
        """Handle finished."""
        print("received finished message")
        if self.task_info["task_state"] == "mapping":
            print("this finished task is a map task")
            self.task_info["receive_count"] += 1
            print("finished map task num:")
            print(self.task_info["receive_count"])
            print("map tasks total num:")
            print(self.current_job["num_mappers"])
            if self.task_info[
                    "receive_count"] == self.current_job["num_mappers"]:
                self.task_info["task_state"] = "map_finished"
                print("mapping finished!")
        elif self.task_info["task_state"] == "reducing":
            print("this finished task is a reduce task")
            self.task_info["finish_count"] += 1
            print(self.task_info["finish_count"])
            print(self.current_job["num_reducers"])
            if self.task_info[
                    "finish_count"] == self.current_job["num_reducers"]:
                self.task_info["task_state"] = "reduce_finished"
        # use else here for both new manager job and finish call, call the
        # handle job here because in this case the momnet received the last
        # finished from mapping, we can directly call reducing the thing of
        # check job q change to that if q is not empty and manager free now
        # call the handle job, create a senario for the new execution

        # change the worker's state to ready again:
        pid = self.get_worker_id(message_dict["worker_host"],
                                 message_dict["worker_port"])
        # with self.worker_state_lock:
        self.update_ready(pid)
        # TODOO: race condition?
        LOGGER.info("finished update ready")

    def get_free_workers(self):
        """Get free workers."""
        have_free_workers = False
        self.workers_info["free_workers"].clear()
        for w_id, worker in self.workers_info["workers"].items():
            if worker.state == "ready":
                have_free_workers = True
                self.workers_info["free_workers"][w_id] = worker
        print("now we have free workers, num:")
        print(len(self.workers_info["free_workers"]))
        print("now we have total workers, num:")
        print(len(self.workers_info["workers"]))
        return have_free_workers

    def sorting(self, input_list, num_workers, num_files, tasks, task_type):
        """Sort input list."""
        # sort input list
        sorted_input_list = sorted(input_list)
        # create numTasks of lists and add them to the list of tasks
        for i in range(num_workers):
            if task_type == 'map':
                tasks.append(
                    {'task_type': 'map', 'task_id': i, 'task_files': []})
            elif task_type == 'reduce':
                tasks.append(
                    {'task_type': 'reduce', 'task_id': i, 'task_files': []})
        # then iterate through all files and assign them to workers
        for i in range(num_files):
            tasks[i % num_workers]['task_files'].append(sorted_input_list[i])
        # tasks is now a list of "task_dict"

    def update_busy(self, worker_id):
        """Update busy."""
        self.workers_info["workers"][worker_id].state = "busy"

    def update_ready(self, worker_id):
        """Update ready."""
        self.workers_info["workers"][worker_id].state = "ready"

    # for mapping:
    def partition_mapping(self, message_dict):
        """Partition mapping tasks."""
        input_list = []
        for file in Path(message_dict["input_directory"]).iterdir():
            input_list.append(str(file))
        num_needed_mappers = message_dict["num_mappers"]
        num_files = len(input_list)

        # sort the files and tasks:
        # TODOO: check correctness?
        partitioned_tasks = []
        self.sorting(input_list, num_needed_mappers,
                     num_files, partitioned_tasks, "map")
        # partitioned tasks is now a list of "task_dict"
        for partitioned_task in partitioned_tasks:
            self.task_info["map_tasks"].append(partitioned_task)
        print("finished partioning map tasks")
        # now self.task_info["map_tasks"] added num_needed_mappers entries
        self.task_info["task_state"] = "mapping"

    def assign_map_work(self, message_dict, tmpdir):
        """Assign map tasks."""
        # if finished map, return
        if self.task_info["receive_count"] == self.current_job["num_mappers"]:
            return
        # if no available map task, return
        if len(self.task_info["map_tasks"]) == 0:
            return
        print("starting assigning map tasks")
        num_reducers = message_dict["num_reducers"]
        executable = message_dict["mapper_executable"]

        print("we're going into the map loop")

        # tasks will be a list of (list of filename strings)
        # now tasks have num_needed_mappers entries
        while len(self.task_info[
            "map_tasks"]) != 0 and not self.manager_info["shutdown"] and\
                self.task_info["task_state"] == "mapping":
            sleep(0.1)
            print("we are in the map loop")
            # update free worker each loop!!!!
            self.get_free_workers()
            print("trying to assigning tasks to mappers")
            for worker_id, worker in self.workers_info["free_workers"].items():
                print("free worker num?")
                print(len(self.workers_info["free_workers"]))
                print("available map task num?")
                print(len(self.task_info["map_tasks"]))
                if len(self.task_info["map_tasks"]) == 0:
                    break
                print(worker.worker_host)
                print(worker.worker_port)
                task = self.task_info["map_tasks"].popleft()
                # update worker's state to busy BEFORE messaging
                self.update_busy(worker_id)
                # TODOO: could have race condition, so lock this!
                # don't want sb to modify this before it update_busy
                # add an if statement?
                LOGGER.info("finished update busy")
                self.workers_info["workers"][
                    worker_id].current_task = task
                # with self.worker_state_lock:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as\
                        sock:
                    host = worker.worker_host
                    port = worker.worker_port
                    try:
                        sock.connect((host, port))
                        message = json.dumps({
                            "message_type": "new_map_task",
                            "task_id": task['task_id'],
                            # list of filename strings
                            "input_paths": task['task_files'],
                            "executable": executable,
                            "output_directory": tmpdir,
                            "num_partitions": num_reducers,
                            "worker_host": host,
                            "worker_port": port
                        })
                        print(message)
                        sock.sendall(message.encode('utf-8'))
                    except ConnectionRefusedError:
                        LOGGER.info("ConnectionRefusedError")
                        self.mark_worker_dead(worker_id)
                        self.task_info["map_tasks"].appendleft(task)
                        print("dead")

    # for reducing:
    def partition_reducing(self, message_dict, tmpdir):
        """Partition reducing."""
        # open the temp_dir it create and use the filename inside
        # use str(file) to turn the file name just to string

        all_paths = []
        for file in Path(tmpdir).iterdir():
            all_paths.append(str(file))

        num_needed_reducers = message_dict["num_reducers"]
        num_files = len(all_paths)

        # sort the files and tasks for the workers:
        partitioned_tasks = []
        self.sorting(all_paths, num_needed_reducers,
                     num_files, partitioned_tasks, "reduce")
        for partitioned_task in partitioned_tasks:
            self.task_info["reduce_tasks"].append(partitioned_task)
        print("finished partioning reduce tasks")
        # now self.task_info["reduce_tasks"] added num_needed_reducers entries
        self.task_info["task_state"] = "reducing"

    def assign_reduce_work(self, message_dict):
        """Assign reduce work."""
        # if finished reduce, return
        if self.task_info["finish_count"] == self.current_job["num_reducers"]:
            return
        # if no available reduce task, return
        if len(self.task_info["reduce_tasks"]) == 0:
            return
        print("starting assigning reduce tasks")

        # use files in temp_dir as input_paths(filename) to pass to workers
        # the manager also creates a temp output
        executable = message_dict["reducer_executable"]
        output_directory = message_dict["output_directory"]
        # tasks will be a list of (list of filename strings)

        # send the message to reducers:
        while len(self.task_info[
            "reduce_tasks"]) != 0 and not self.manager_info["shutdown"] and\
                self.task_info["task_state"] == "reducing":
            sleep(0.1)
            # update free worker each loop!!!!
            self.get_free_workers()
            print("trying to assigning tasks to reducers")
            for worker_id, worker in self.workers_info["free_workers"].items():
                print("Assigning tasks to reducers")
                # break if empty!!!!!!!
                if len(self.task_info["reduce_tasks"]) == 0:
                    break
                task = self.task_info["reduce_tasks"].popleft()
                # update worker's state to busy BEFORE messaging
                self.update_busy(worker_id)
                LOGGER.info("finished update busy")
                self.workers_info["workers"][
                    worker_id].current_task = task
                # with self.worker_state_lock:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as\
                        sock:
                    host = worker.worker_host
                    port = worker.worker_port
                    try:
                        sock.connect((host, port))
                        message = json.dumps({
                            "message_type": "new_reduce_task",
                            "task_id": task['task_id'],
                            "executable": executable,
                            "input_paths": task['task_files'],
                            "output_directory": output_directory,
                            "worker_host": host,
                            "worker_port": port
                        })
                        print(message)
                        sock.sendall(message.encode('utf-8'))
                    except ConnectionRefusedError:
                        LOGGER.info("ConnectionRefusedError")
                        self.mark_worker_dead(worker_id)
                        self.task_info["reduce_tasks"].appendleft(task)
                        print("dead")

    # a function for listening to heartbeat messages:
    def listen_hb(self):
        """Listen to workers' heartbeat messages."""
        print("start listening hb")
        # use UDP
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.manager_info["host"], self.manager_info["port"]))
            sock.settimeout(1)

            while self.manager_info["shutdown"] is not True:
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
                    # loop thru workers and if one hasn't for 10s mark as dead
                    w_host = message_dict["worker_host"]
                    w_port = message_dict["worker_port"]
                    self.workers_info["last_beat"][(w_host, w_port)] = time()
                    LOGGER.info(
                        'Received heartbeat from Worker (%s, %s)', w_host,
                        w_port)
                    # still need create func to reassign works of dead workers
        print("listening hb finished")

    def check_dead(self):
        """Check if dead."""
        print("start checking dead")
        while self.manager_info["shutdown"] is not True:
            sleep(0.1)
            for w_id, worker in self.workers_info["workers"].items():
                w_host = worker.worker_host
                w_port = worker.worker_port
                if worker.state != "dead" and\
                        time() - self.workers_info[
                            "last_beat"][(w_host, w_port)] >= 10:
                    LOGGER.info("Some worker died")
                    LOGGER.info(worker.state)
                    if worker.state == "busy":
                        LOGGER.info("Dead worker is busy")
                        # hand over task
                        if worker.current_task['task_type'] == "map":
                            self.task_info[
                                "map_tasks"].append(worker.current_task)
                            LOGGER.info("New map task added")
                        elif worker.current_task['task_type'] == "reduce":
                            self.task_info[
                                "reduce_tasks"].append(worker.current_task)
                            LOGGER.info("New reduce task added")
                    # after checking its state, mark it as dead!!!
                    self.mark_worker_dead(w_id)
                # self.worker_state_lock.release()
        print("checking dead finished")

    def get_worker_id(self, host, port):
        """Get worker id."""
        # consider revive!
        for pid, worker in self.workers_info["workers"].items():
            if worker.worker_host == host and worker.worker_port == port and\
                    worker.state != "dead":
                return pid
        return None

    def mark_worker_dead(self, dead_id):
        """Mark worker as dead."""
        self.workers_info["workers"][dead_id].state = "dead"


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
@click.option("--shared_dir", "shared_dir", default=None)
def main(host, port, logfile, loglevel, shared_dir):
    """Run Manager."""
    tempfile.temp_dir = shared_dir
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
