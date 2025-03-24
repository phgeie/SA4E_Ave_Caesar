import json
import sys
import subprocess
from kafka import KafkaProducer
import time
import random

KAFKA_BROKERS = ["localhost:29092", "localhost:39092", "localhost:49092"]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def read_json_and_run_cli(file_path, logging):
    with open(file_path, 'r') as file:
        data = json.load(file)

    segments = []
    caesar_segment_id = None

    start_goals = 0
    segment_cnt = 0
    for track in data.get("tracks", []):
        for segment in track.get("segments", []):
            if segment["type"] == "caesar":
                caesar_segment_id = segment["segmentId"]
            if segment["type"] == "start-goal":
                start_goals += 1
            segment_cnt += 1



    for track in data.get("tracks", []):
        track_id = int(track["trackId"])
        for segment in track.get("segments", []):
            segment_id = segment["segmentId"]
            segment_type = segment["type"]
            next_segments = ",".join(segment.get("nextSegments", []))

            command = ["python", "segment.py", segment_id, segment_type, next_segments, caesar_segment_id, str(start_goals), str(segment_cnt), logging]
            print(f"Running: {' '.join(command)}")
            process = subprocess.Popen(command)
            segments.append((segment, process, track_id))

    time.sleep(2)

    print()
    print()
    print("Sending start events:")
    print()

    tokens = []
    for segment, process, track_id in segments:
        if segment["type"] == "start-goal":
            cards = [random.randint(1, 6) for _ in range(3)]
            cards = sorted(cards)
            token = {"id": track_id, "round": 0, "visited_caesar": False, "cards": cards}
            print(f"Player {track_id} starts at {segment['segmentId']} with cards {cards}")
            tokens.append(token)
    print()

    for token in tokens:
        producer.send(segment.get("segmentId"), {"event": "start", "token": token})
        time.sleep(0.1)

    for segment, process, track_id in segments:
        process.wait()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python run.py <json_file>")
        sys.exit(1)
    logging = ""
    if len(sys.argv) == 3:
        if sys.argv[2] == "logging":
            logging = "logging"

    json_file = sys.argv[1]
    read_json_and_run_cli(json_file, logging)
