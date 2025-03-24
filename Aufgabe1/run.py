import json
import sys
import subprocess
from kafka import KafkaProducer
import time

KAFKA_BROKERS = ["localhost:29092"]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def read_json_and_run_cli(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)

    segments = []

    for track in data.get("tracks", []):
        track_id = track["trackId"]
        for segment in track.get("segments", []):
            segment_id = segment["segmentId"]
            segment_type = segment["type"]
            next_segments = ",".join(segment.get("nextSegments", []))

            # Start segment.py with the caesar segment ID as an extra argument
            command = ["python", "segment.py", segment_id, segment_type, next_segments]
            print(f"Running: {' '.join(command)}")
            process = subprocess.Popen(command)
            segments.append((segment, process, track_id))

    time.sleep(2)  # Wait for all segments to start
    print()
    print()
    print("Sending start events:")
    print()

    for segment, process, track_id in segments:
        if segment["type"] == "start-goal":
            token = {"id": track_id, "round": 0}
            producer.send(segment.get("segmentId"), {"event": "start", "token": token})
            print(f"Player {token.get('id')} starts at {segment.get('segmentId')}")
    print()

    for segment, process, track_id in segments:
        process.wait()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python read_json_and_run_cli.py <json_file>")
        sys.exit(1)

    json_file = sys.argv[1]
    read_json_and_run_cli(json_file)
