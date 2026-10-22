"""Wait for real scheduled workers in tests without making dispatch synchronous."""
import time

def finish_scheduler(scheduler,timeout=60):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        with scheduler._lock:
            threads=[entry[0] for entry in scheduler._running.values()]
        if not threads:return
        for thread in threads:thread.join(timeout=min(.1,max(0,deadline-time.monotonic())))
    raise AssertionError('Scheduled workers did not finish')
