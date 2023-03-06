"""A class for profiling memory usage during unit tests."""
import resource
import threading
import sys
import time


class MemoryProfiler:
    """Monitor memory usage in a separate thread."""

    # Time between memory usage measurements in s
    PROFILE_INTERVAL = 0.05

    def __init__(self):
        """Start profiling."""
        self.run = True  # stop var
        self.mem_before = None
        self.mem_max = None
        self.time_start = None
        self.time_stop = None
        self.profile_thread = None

    def profile(self):
        """Measure memory usage periodically and store the max.

        This function runs in a separate thread.
        """
        self.mem_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        self.mem_max = self.mem_before
        while self.run:
            mem_cur = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            self.mem_max = max(mem_cur, self.mem_max)
            time.sleep(self.PROFILE_INTERVAL)

    def start(self):
        """Start profiler in a separate thread."""
        self.profile_thread = threading.Thread(target=self.profile)
        self.time_start = time.time()
        self.profile_thread.start()

    def stop(self):
        """Stop profiler."""
        self.time_stop = time.time()
        self.run = False
        self.profile_thread.join()

    def get_mem_delta(self):
        """Return max difference in memory usage (B) since start."""
        # macOS returns memory in B
        if sys.platform == "darwin":
            return self.mem_max - self.mem_before

        # Linux returns kB, convert to B
        if sys.platform == "linux":
            return (self.mem_max - self.mem_before) * 1024

        # Should never get here
        raise SystemError(f"Unsupported platform {sys.platform}")

    def get_time_delta(self):
        """Return time difference in seconds from start to stop."""
        return self.time_stop - self.time_start
