import os
import json
import time
from datetime import datetime
from kafka import KafkaConsumer
import boto3
from dotenv import load_dotenv

def consume_and_write_to_s3(
    topic,
    s3_prefix,
    group_id="kafka_to_s3_group",
    max_idle_seconds=10,
    poll_interval=1,
    batch_size=100
):
    """
    Consumes messages from a given Kafka topic using consumer.poll(),
    writes them in batches to S3, and stops if no new messages arrive
    for 'max_idle_seconds'.
    """
    load_dotenv()

    # Set up S3 client from environment
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION")
    )
    bucket = os.getenv("S3_BUCKET_NAME")
    if not bucket:
        print("S3_BUCKET_NAME is not set in your environment.")
        return

    # Create Kafka consumer
    consumer = KafkaConsumer(
        bootstrap_servers="kafka1:9092",
        auto_offset_reset="earliest",
        group_id=group_id,
        # poll() returns a dict of {TopicPartition: [messages]}
        # We'll decode messages ourselves below
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

    # Subscribe to the given topic
    consumer.subscribe([topic])

    idle_time = 0  # how long we've gone without any new messages
    batch = []
    print(f"\n--- Consuming from '{topic}' → s3://{bucket}/{s3_prefix}/ ---")
    print(f"Will stop if no new messages for {max_idle_seconds} seconds.")

    try:
        while True:
            # Poll returns a dict: {TopicPartition(...): [ConsumerRecord, ...], ...}
            msg_pack = consumer.poll(timeout_ms=poll_interval * 1000)

            if not msg_pack:
                # No messages this poll, increment idle time
                idle_time += poll_interval
                if idle_time >= max_idle_seconds:
                    print(f"No new messages for {idle_time}s, stopping consumer for {topic}.")
                    break
            else:
                # Reset idle time whenever we get new messages
                idle_time = 0

                # Process the messages
                for tp, messages in msg_pack.items():
                    for msg in messages:
                        batch.append(msg.value)
                        if len(batch) >= batch_size:
                            _write_batch_to_s3(s3, bucket, s3_prefix, batch)
                            batch.clear()

        # After breaking out of loop, flush any remaining messages
        if batch:
            _write_batch_to_s3(s3, bucket, s3_prefix, batch)
            batch.clear()

    except KeyboardInterrupt:
        print("Interrupted by user, flushing any remaining messages...")
        if batch:
            _write_batch_to_s3(s3, bucket, s3_prefix, batch)
            batch.clear()

    finally:
        consumer.close()
        print(f"Consumer for topic '{topic}' closed.")

def _write_batch_to_s3(s3_client, bucket, s3_prefix, batch):
    """
    Helper function to write a batch of messages to S3 as a single JSON file.
    Uses microseconds in the filename to avoid overwrites.
    """
    now_utc = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    file_key = f"{s3_prefix}/{now_utc}.json"

    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=file_key,
            Body=json.dumps(batch)
        )
        print(f"Wrote {len(batch)} records to s3://{bucket}/{file_key}")
    except Exception as e:
        print(f"Error writing batch to s3://{bucket}/{file_key}: {e}")

def main():
    # 1) Consume from 'spotify_eu_tracks'
    consume_and_write_to_s3(
        topic="spotify_eu_tracks",
        s3_prefix="spotify/bronze/eu_tracks",
        group_id="kafka_to_s3_tracks",
        max_idle_seconds=10,   # stop after 60s of no new messages
        poll_interval=1,       # poll every 5s
        batch_size=100
    )

    # 2) Then consume from 'spotify_eu_artists'
    consume_and_write_to_s3(
        topic="spotify_eu_artists",
        s3_prefix="spotify/bronze/eu_artists",
        group_id="kafka_to_s3_artists",
        max_idle_seconds=10,
        poll_interval=1,
        batch_size=100
    )

if __name__ == "__main__":
    main()
