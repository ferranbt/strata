#!/usr/bin/env python3
"""Smoke-test client for the strata Arrow Flight server.

Lists every flight, then fetches a few endpoints and prints the returned rows.
Exercises single-object, array, and nested list<struct> conversions.

Usage:
    # 1. start the server
    cargo run -- serve --addr 127.0.0.1:50071

    # 2. run this client (needs `pyarrow`; pandas not required)
    python3 -m venv /tmp/flightvenv && /tmp/flightvenv/bin/pip install pyarrow
    /tmp/flightvenv/bin/python src/flight/utils/flight_client.py [grpc://host:port]
"""

import sys
import time

import pyarrow.flight as fl

addr = sys.argv[1] if len(sys.argv) > 1 else "grpc://127.0.0.1:50071"

client = fl.connect(addr)
for _ in range(40):
    try:
        client.wait_for_available(timeout=1)
        break
    except Exception:
        time.sleep(0.25)

print("=== list_flights ===")
for f in client.list_flights():
    path = [p.decode() for p in f.descriptor.path]
    ncols = len(f.schema) if f.schema is not None else 0
    print(f"  {path}  ({ncols} cols)")


def fetch(provider, path):
    print(f"\n=== {provider} {path} ===")
    desc = fl.FlightDescriptor.for_path(provider, path)
    info = client.get_flight_info(desc)
    print("schema:", [(field.name, str(field.type)) for field in info.schema])
    reader = client.do_get(info.endpoints[0].ticket)
    table = reader.read_all()
    print(f"rows={table.num_rows}")
    for row in table.to_pylist()[:5]:
        print("  ", {k: (str(v)[:50] if v is not None else None) for k, v in row.items()})


if __name__ == "__main__":
    fetch("github", "/users/torvalds")
    fetch("github", "/repos/rust-lang/rust")
    fetch("spotify", "/albums/4m2880jivSbbyEGAKfITCa/tracks")
