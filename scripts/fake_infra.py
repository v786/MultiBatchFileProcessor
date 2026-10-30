"""Stand-ins for Redis and S3 when Docker is not available: fakeredis over TCP and moto's S3 server.

    python scripts/fake_infra.py --redis-port 6390 --s3-port 9010

Postgres is replaced by SQLite (DATABASE_URL=sqlite:///...). Good enough to click through the app and run the
multi-process pipeline; use `docker compose up` for the real thing.
"""

import argparse
import threading

from fakeredis import TcpFakeServer
from moto.server import ThreadedMotoServer


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--redis-port", type=int, default=6390)
    p.add_argument("--s3-port", type=int, default=9010)
    args = p.parse_args()

    moto = ThreadedMotoServer(ip_address="127.0.0.1", port=args.s3_port)
    moto.start()
    server = TcpFakeServer(("127.0.0.1", args.redis_port), server_type="redis")
    print(f"fake redis on :{args.redis_port}, fake S3 on :{args.s3_port}", flush=True)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    threading.Event().wait()


if __name__ == "__main__":
    main()
