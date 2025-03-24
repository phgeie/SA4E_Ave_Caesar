import sys
import json
import time
import threading
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BROKERS = ["localhost:29092"]
MAX_ROUNDS = 3  # Token must complete exactly 3 rounds

class Segment:
    def __init__(self, segment_id, segment_type, next_segments):
        self.segment_id = segment_id
        self.segment_type = segment_type
        self.next_segments = next_segments

        self.consumer = KafkaConsumer(
            segment_id,
            bootstrap_servers=KAFKA_BROKERS,
            value_deserializer=lambda m: json.loads(m.decode("utf-8"))
        )

        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

    def handle_message(self, msg):
        player = msg.get("token")
        event = msg.get("event")

        print(f"Segment {self.segment_id} received event '{event}' with token: {player}")

        if event == "start":
            # Initialize timer on first entry
            if "start_time" not in player:
                player["start_time"] = time.time()

            if self.segment_type == "start-goal":
                player["round"] += 1

            # Stop the token after exactly 3 rounds and calculate elapsed time
            if player["round"] > MAX_ROUNDS:
                end_time = time.time()
                elapsed_time = end_time - player["start_time"]
                print(f"Token {player['id']} completed {MAX_ROUNDS} rounds in {elapsed_time:.5f} seconds.")
                return  # Token stops

            # Normal movement rules
            next_segment = self.next_segments[0]
            if next_segment:
                print(f"Forwarding token {player['id']} to {next_segment} (round {player['round']})")
                self.move_token(next_segment, player)

            time.sleep(1)  # Simulate delay before moving


    def move_token(self, target_segment, player):
        self.producer.send(target_segment, value={"event": "start", "token": player})
        time.sleep(.1)


if __name__ == "__main__":
    segment_id = sys.argv[1]
    segment_type = sys.argv[2]
    next_segments = sys.argv[3].split(",")

    segment = Segment(segment_id, segment_type, next_segments)

    try:
        for msg in segment.consumer:
            thread = threading.Thread(target=segment.handle_message, args=(msg.value,))
            thread.start()
    except KeyboardInterrupt:
        print("\n[INFO] Exiting gracefully...")
        sys.exit(0)
