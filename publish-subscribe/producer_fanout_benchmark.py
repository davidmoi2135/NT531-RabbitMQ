#!/usr/bin/env python3
import argparse
import json
import os
import time
import uuid

import pika


def parse_args():
    p = argparse.ArgumentParser(description="RabbitMQ Fanout Producer Benchmark")
    p.add_argument("--host", default=os.getenv("RABBIT_HOST", "192.168.1.132"))
    p.add_argument("--port", type=int, default=int(os.getenv("RABBIT_PORT", "5672")))
    p.add_argument("--user", default=os.getenv("RABBIT_USER", "admin"))
    p.add_argument("--password", default=os.getenv("RABBIT_PASS", "admin123"))

    p.add_argument("--exchange", default=os.getenv("EXCHANGE_NAME", "broadcast_x"))
    p.add_argument("-n", "--messages", type=int, default=30000)
    p.add_argument("--payload-bytes", type=int, default=256)
    p.add_argument("--rate", type=float, default=0.0, help="msg/s (0=unlimited)")
    p.add_argument("--durable", action="store_true", help="declare durable exchange")
    p.add_argument("--persistent", action="store_true")
    p.add_argument("--confirm", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()

    creds = pika.PlainCredentials(args.user, args.password)
    params = pika.ConnectionParameters(
        host=args.host,
        port=args.port,
        credentials=creds,
        heartbeat=30,
        blocked_connection_timeout=60,
    )

    conn = pika.BlockingConnection(params)
    ch = conn.channel()

    ch.exchange_declare(
        exchange=args.exchange,
        exchange_type="fanout",
        durable=args.durable,
    )

    if args.confirm:
        ch.confirm_delivery()

    props = pika.BasicProperties(content_type="application/json")
    if args.persistent:
        props.delivery_mode = 2

    interval = (1.0 / args.rate) if args.rate and args.rate > 0 else 0.0
    next_send = time.perf_counter()

    run_id = uuid.uuid4().hex[:10]
    t0 = time.perf_counter()

    confirm_fail = 0
    published_ok = 0

    for i in range(args.messages):
        payload = {
            "run_id": run_id,
            "seq": i,
            "sent_ts": time.time(),
            "body": "x" * max(0, args.payload_bytes - 64),
        }
        body = json.dumps(payload).encode("utf-8")

        try:
            ch.basic_publish(
                exchange=args.exchange,
                routing_key="",
                body=body,
                properties=props,
            )
            published_ok += 1
        except Exception:
            if args.confirm:
                confirm_fail += 1
            else:
                raise

        if interval > 0:
            next_send += interval
            now = time.perf_counter()
            if next_send > now:
                time.sleep(next_send - now)
            else:
                next_send = now

    t1 = time.perf_counter()
    dur = t1 - t0
    thr = published_ok / dur if dur > 0 else 0.0

    print("=== Fanout Producer Benchmark Result ===")
    print(f"host: {args.host}:{args.port}")
    print(f"exchange: {args.exchange} (fanout)")
    print(f"messages: {args.messages}")
    print(f"payload_bytes: ~{args.payload_bytes}")
    print(f"persistent_msg: {args.persistent}")
    print(f"publisher_confirms: {args.confirm}")
    print(f"rate_limit: {args.rate} msg/s (0 = unlimited)")
    print(f"run_id: {run_id}")
    print(f"duration_sec: {dur:.4f}")
    print(f"published_ok: {published_ok}")
    print(f"throughput_msg_per_sec: {thr:.2f}")
    print(f"confirm_fail: {confirm_fail}")

    conn.close()

if __name__ == "__main__":
    main()
