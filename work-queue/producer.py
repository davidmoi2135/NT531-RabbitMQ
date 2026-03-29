import argparse
import json
import os
import time
import uuid
import pika

def parse_args():
    p = argparse.ArgumentParser(description="RabbitMQ Producer Benchmark")
    p.add_argument("--host", default=os.getenv("RABBIT_HOST", "172.31.47.43"))
    p.add_argument("--port", type=int, default=int(os.getenv("RABBIT_PORT", "5672")))
    p.add_argument("--user", default=os.getenv("RABBIT_USER", "admin"))
    p.add_argument("--password", default=os.getenv("RABBIT_PASS", "123456"))
    p.add_argument("--queue", default=os.getenv("QUEUE_NAME", "orders_queue"))

    p.add_argument("-n", "--messages", type=int, default=10000, help="number of messages")
    p.add_argument("--payload-bytes", type=int, default=256, help="payload size in bytes")
    p.add_argument("--rate", type=float, default=0.0, help="rate limit msg/s (0=unlimited)")
    p.add_argument("--durable", action="store_true", help="declare durable queue")
    p.add_argument("--persistent", action="store_true", help="set delivery_mode=2")
    p.add_argument("--confirm", action="store_true", help="publisher confirms")
    return p.parse_args()

def main():
    args = parse_args()
    creds = pika.PlainCredentials(args.user, args.password)
    params = pika.ConnectionParameters(
        host=args.host, port=args.port, credentials=creds,
        heartbeat=30, blocked_connection_timeout=60
    )
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.queue_declare(queue=args.queue, durable=args.durable)
    if args.confirm:
        ch.confirm_delivery()

    props = pika.BasicProperties(content_type="application/json")
    if args.persistent:
        props.delivery_mode = 2

    interval = (1.0 / args.rate) if args.rate and args.rate > 0 else 0.0
    next_send = time.perf_counter()
    run_id = uuid.uuid4().hex[:10]
    confirm_fail = 0

    t0 = time.perf_counter()
    for i in range(args.messages):
        payload = {
            "run_id": run_id, "seq": i, "sent_ts": time.time(),
            "body": "x" * max(0, args.payload_bytes - 64),
        }
        body = json.dumps(payload).encode("utf-8")
        if args.confirm:
            ok = ch.basic_publish(exchange="", routing_key=args.queue, body=body, properties=props, mandatory=False)
            if not ok: confirm_fail += 1
        else:
            ch.basic_publish(exchange="", routing_key=args.queue, body=body, properties=props)

        if interval > 0:
            next_send += interval
            now = time.perf_counter()
            if next_send > now: time.sleep(next_send - now)
            else: next_send = now

    t1 = time.perf_counter()
    duration = t1 - t0
    throughput = args.messages / duration if duration > 0 else 0.0

    print("=== Producer Benchmark Result ===")
    print(f"host: {args.host}:{args.port}")
    print(f"queue: {args.queue}")
    print(f"messages: {args.messages}")
    print(f"payload_bytes: ~{args.payload_bytes}")
    print(f"durable_queue: {args.durable}")
    print(f"persistent_msg: {args.persistent}")
    print(f"publisher_confirms: {args.confirm}")
    print(f"rate_limit: {args.rate} msg/s (0 = unlimited)")
    print(f"run_id: {run_id}")
    print(f"duration_sec: {duration:.4f}")
    print(f"throughput_msg_per_sec: {throughput:.2f}")
    print(f"confirm_fail: {confirm_fail}")
    conn.close()

if __name__ == "__main__":
    main()
