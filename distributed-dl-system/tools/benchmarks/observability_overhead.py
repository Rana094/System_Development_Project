"""Offline event-emission microbenchmark; no controller imports, DB, network or Pi.

JSON on stdout. Measures synchronous JSON encoding/logging to memory, not disk
rotation, remote logging, NFS throughput, UI latency or total training overhead.
"""
import argparse
import io
import json
import logging
from pathlib import Path
import platform
import statistics
import sys
import time

sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
from hybrid_storage import events


def run(iterations,repeats):
    context=dict(job_id='synthetic-overhead',attempt_id='synthetic-overhead:0',worker_id='pi02',rank=0,
                 stage_id='a'*32,manifest_version='b'*64,manifest_schema_version=1,dataset_version=1,
                 cache_hit_bytes=0,transferred_bytes=1048576,verified_bytes=1048576,
                 staging_duration_ns=100000000,verification_duration_ns=1000000,copy_duration_ns=90000000)
    logger=events.LOGGER;state=(logger.level,logger.handlers[:],logger.propagate,logger.disabled)
    samples={k:[] for k in ('empty_call','disabled','enabled_memory_jsonl')};counts=[]
    def empty():return None
    def emit():return events.emit('storage.staging.ready',**context)
    try:
        logger.propagate=False;logger.disabled=False
        for _ in range(repeats):
            for name in samples:
                stream=io.StringIO();handler=logging.StreamHandler(stream);handler.setFormatter(logging.Formatter('%(message)s'))
                logger.handlers=[handler];logger.setLevel(logging.INFO if name=='enabled_memory_jsonl' else logging.WARNING)
                action=empty if name=='empty_call' else emit
                start=time.perf_counter_ns()
                for _ in range(iterations):action()
                samples[name].append((time.perf_counter_ns()-start)/iterations)
                if name=='enabled_memory_jsonl':
                    rows=stream.getvalue().splitlines();assert len(rows)==iterations
                    assert all(json.loads(row)['verified_bytes']==1048576 for row in rows)
                    counts.append(len(rows))
                else:assert not stream.getvalue()
                handler.close();stream.close()
    finally:
        logger.setLevel(state[0]);logger.handlers=state[1];logger.propagate=state[2];logger.disabled=state[3]
    return dict(schema_version=1,kind='synthetic_event_microbenchmark',measured_at_ns=time.time_ns(),
        python=platform.python_version(),platform=platform.platform(),iterations=iterations,repeats=repeats,
        ns_per_call={k:dict(median=statistics.median(v),minimum=min(v),maximum=max(v),samples=v) for k,v in samples.items()},
        validated_jsonl_records=sum(counts),units='nanoseconds per synchronous call',
        limits='Memory JSONL sink only; not disk/network/NFS/Pi/UI or total training overhead; no performance pass threshold.')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--iterations',type=int,default=5000);parser.add_argument('--repeats',type=int,default=5)
    args=parser.parse_args()
    if not 1<=args.iterations<=100000 or not 1<=args.repeats<=20:parser.error('Expected iterations 1..100000, repeats 1..20')
    print(json.dumps(run(args.iterations,args.repeats),indent=2,allow_nan=False))


if __name__=='__main__':main()
