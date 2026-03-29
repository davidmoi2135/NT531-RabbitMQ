#worker.py - RabbitMQ worker with prefetch=1 and manual ack
#!/usr/bin/env python3
import argparse
import json
import os
import sys
import time
import pika

def parse_args():
    p = argparse.ArgumentParser(description="RabbitMQ Worker (prefetch=1, manual ack)")
    p.add_argument("--host", default=os.getenv("RABBIT_HOST", "192.168.1.133"))
    p.add_argument("--port", type=int, default=int(os.getenv("RABBIT_PORT", "5672")))
    p.add_argument("--user", default=os.getenv("RABBIT_USER", "admin"))
    p.add_argument("--password", default=os.getenv("RABBIT_PASS", "admin123"))
    p.add_argument("--queue", default=os.getenv("QUEUE_NAME", "orders_queue"))

    p.add_argument("--worker-id", default=os.getenv("WORKER_ID", "1"))
    p.add_argument("--sleep-ms", type=int, default=int(os.getenv("SLEEP_MS", "20")))
    p.add_argument("--prefetch", type=int, default=int(os.getenv("PREFETCH", "1")))
    p.add_argument("--queue-durable", type=int, default=int(os.getenv("QUEUE_DURABLE", "1")),
               help="1=durable queue, 0=non-durable")
    p.add_argument("--log", default="", help="log file path (optional)")
    return p.parse_args()

def log_line(fp, s: str):
    fp.write(s + "\n")
    fp.flush()

def main():
    args = parse_args()
    out = open(args.log, "a", buffering=1) if args.log else sys.stdout

    creds = pika.PlainCredentials(args.user, args.password)
    params = pika.ConnectionParameters(
        host=args.host, port=args.port, credentials=creds,
        heartbeat=30, blocked_connection_timeout=60
    )
    conn = pika.BlockingConnection(params)
    ch = conn.channel()

    ch.queue_declare(queue=args.queue, durable=bool(args.queue_durable))
    ch.basic_qos(prefetch_count=args.prefetch)

    sleep_s = args.sleep_ms / 1000.0
    log_line(out, f"[worker {args.worker_id}] started queue={args.queue} prefetch={args.prefetch} sleep_ms={args.sleep_ms}")

    def on_message(channel, method, properties, body: bytes):
        recv_ts = time.time()
        start = time.perf_counter()
        try:
            msg = json.loads(body.decode("utf-8"))
            run_id = msg.get("run_id", "")
            seq = msg.get("seq", -1)
            sent_ts = float(msg.get("sent_ts", 0.0)) if msg.get("sent_ts") else 0.0
            net_latency_ms = (recv_ts - sent_ts) * 1000.0 if sent_ts > 0 else -1.0

            time.sleep(sleep_s)
            proc_ms = (time.perf_counter() - start) * 1000.0

            log_line(out, json.dumps({
                "worker_id": args.worker_id,
                "run_id": run_id,
                "seq": seq,
                "net_latency_ms": round(net_latency_ms, 3),
                "proc_ms": round(proc_ms, 3),
                "acked": True,
                "ts": recv_ts,
            }))
            channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            log_line(out, f"[worker {args.worker_id}] ERROR: {e}")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    ch.basic_consume(queue=args.queue, on_message_callback=on_message, auto_ack=False)
    ch.start_consuming()

if __name__ == "__main__":
    main()
