Just finished a simple test case for heartbeat.

I hard code created a worker and had it send an initial heartbeat then send the
next heartbeat 15 seconds later. This will cause the worker to die 11 seconds
after its first heartbeat.

To run on command line, run python3 master.py