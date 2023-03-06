"""Shared test fixtures.

Pytest fixture docs:
https://docs.pytest.org/en/latest/fixture.html#conftest-py-sharing-fixture-functions
"""

import unittest.mock
import threading
import functools
import collections
import contextlib
import logging
import shutil
import socket
import subprocess
import time
import sys
import pytest
import utils


# Set up logging
LOGGER = logging.getLogger("autograder")

# How long to wait for server in separate process to start or stop
TIMEOUT = 5

# Number of Workers which will run in separate processes
N_WORKERS = 3

# The mapreduce_client fixture will return a MapReduceClient object
MapReduceClient = collections.namedtuple("MapReduceClient", [
    "manager_host",
    "manager_port",
])


@pytest.fixture(name='mapreduce_client')
def setup_teardown_mapreduce_client():
    """Start a MapReduce Manager and Worker Servers in separate processes."""
    LOGGER.info("Setup test fixture 'mapreduce_client'")

    # Acquire open ports
    manager_port, *worker_ports = \
        utils.get_open_port(nports=1 + N_WORKERS)

    # Match the server log level to the current log level.  For that we
    # need to access a protected member of the logging library.
    # pylint: disable=protected-access
    loglevel = logging._levelToName[logging.root.level]

    # Each server is a LiveServer object whose lifetime is automatically
    # managed by a context manager
    with contextlib.ExitStack() as stack:
        processes = []

        # Start Manager
        LOGGER.info("Starting Manager")
        process = stack.enter_context(subprocess.Popen([
            shutil.which("mapreduce-manager"),
            "--port", str(manager_port),
            "--loglevel", loglevel,
        ]))
        processes.append(process)
        wait_for_server_ready(process, manager_port)

        # Start Workers
        for worker_port in worker_ports:
            LOGGER.info("Starting Worker")
            process = stack.enter_context(subprocess.Popen([
                shutil.which("mapreduce-worker"),
                "--port", str(worker_port),
                "--manager-port", str(manager_port),
                "--loglevel", loglevel,
            ]))
            processes.append(process)
            wait_for_server_ready(process, worker_port)

        # Transfer control to test.  The code before the "yield" statement is
        # setup code, which is executed before the test.  Code after the
        # "yield" is teardown code, which is executed at the end of the test.
        # Teardown code is executed whether the test passed or failed.
        yield MapReduceClient("localhost", manager_port)

        # Send shutdown message
        utils.send_message({
            "message_type": "shutdown"
        }, port=manager_port)

        # Wait for processes to stop
        wait_for_process_all_stopped(processes)

        # Check for clean exit
        for process in processes:
            assert process.returncode == 0, \
                f"{process} returncode={process.returncode}"

        # Kill servers
        LOGGER.info("Teardown test fixture 'mapreduce_client'")
        for process in processes:
            process.terminate()


def wait_for_server_ready(process, port):
    """Wait for server to respond, raise exception on timeout."""
    for _ in range(10*TIMEOUT):
        if port_is_open("localhost", port):
            return
        if process.poll() is not None:
            raise ChildProcessError(f"Premature exit: {process}")
        time.sleep(0.1)
    raise ChildProcessError(f"Failed to start: {process}")


def port_is_open(host, port):
    """Return True if host:port is open."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.connect((host, port))
        except ConnectionRefusedError:
            return False
        return True


def wait_for_process_all_stopped(processes):
    """Return after all processes are not alive."""
    for _ in range(10*TIMEOUT):
        if all(p.poll() is not None for p in processes):
            return
        time.sleep(0.1)
    raise ChildProcessError("Some processes failed to stop.")


@pytest.fixture(autouse=True)
def thread_safe_mocks():
    """Monkeypatch unittest.mock to be thread-safe.

    As of August 2022, the unittest.mock module is not thread-safe.  Whenever
    code tries accessing an attribute of a mock object, the internal method
    NonCallableMock.__getattr__() is called.  This method either creates a new
    mock object for that attribute or returns one that's already stored. If a
    test and a student solution both access the same attribute of the same mock
    object, a race condition can occur, causing the test and student solution
    to see two different versions of the same object.

    This pytest fixture runs automatically at the beginning of every test. It
    monkeypatches NonCallableMock.__getattr__() to acquire a lock before
    running any other code, which prevents a test and student solution from
    invoking __getattr__() at the same time.  At the end of the test, this
    fixture removes the monkey patch, putting the mock library back in its
    original unpatched state.

    We have made an issue on the CPython repo to fix this problem. See here:
    https://github.com/python/cpython/issues/98624
    This fixture can be removed after the upstream fix is adopted.

    We need this patch to fix a subtle race condition described here:
    https://github.com/eecs485staff/p4-mapreduce/issues/520#issuecomment-1194571187
    https://github.com/eecs485staff/p4-mapreduce/issues/520#issuecomment-1197641647
    https://github.com/eecs485staff/p4-mapreduce/pull/610

    """
    if (
        (sys.version_info.minor == 10 and sys.version_info.micro >= 9)
        or (sys.version_info.minor == 11 and sys.version_info.micro >= 1)
        or sys.version_info.minor >= 12
    ):
        # We don't need this monkeypatch in Python versions released after
        # October 28, 2022 due to this PR:
        # https://github.com/python/cpython/pull/98688
        yield
        return

    # Store the original version of the NonCallableMock class's __getattr__.
    orig_getattr = unittest.mock.NonCallableMock.__getattr__

    # Store a lock as a class variable of NonCallableMock.
    # pylint: disable=protected-access
    unittest.mock.NonCallableMock._lock = threading.RLock()

    # Wrap the original function with one that synchronizes via a lock
    @functools.wraps(orig_getattr)
    def getattr_wrapper(obj, *args, **kwargs):
        """Acquire a lock before calling the original __getattr__ method."""
        with obj._lock:
            return orig_getattr(obj, *args, **kwargs)

    # Set the __getattr__ attribute to our wrapper function.
    unittest.mock.NonCallableMock.__getattr__ = getattr_wrapper

    # Transfer control to test.  The code before the "yield" statement is
    # setup code, which is executed before the test.  Code after the
    # "yield" is teardown code, which is executed at the end of the test.
    # Teardown code is executed whether the test passed or failed.
    yield

    # Now that the fixture has regained control, reset __getattr__ to its
    # original value and remove the _lock attribute we created.
    unittest.mock.NonCallableMock.__getattr__ = orig_getattr
    del unittest.mock.NonCallableMock._lock
