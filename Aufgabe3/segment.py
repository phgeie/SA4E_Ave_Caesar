import sys
import json
import time
import threading
import random
from kafka import KafkaConsumer, KafkaProducer


KAFKA_BROKERS = ["localhost:29092", "localhost:39092", "localhost:49092"]
MAX_ROUNDS = 3

class Segment:
    def __init__(self, segment_id, segment_type, next_segments, caesar_segment_id, start_goals, segment_cnt, logging):
        self.segment_id = segment_id
        self.segment_type = segment_type
        self.next_segments = next_segments
        self.caesar_segment_id = caesar_segment_id
        self.occupied = False
        self.next_is_free = 0
        self.next_free_segment_id = None
        self.current_turnplayer = 1
        self.start_goals = start_goals
        self.segment_cnt = segment_cnt
        self.round_cnt = 0
        self.used_card_index = -1
        self.finished_players = []
        self.occupied_by = 0
        self.logging = logging

        self.consumer = KafkaConsumer(
            segment_id,
            "turn_updates",
            bootstrap_servers=KAFKA_BROKERS,
            value_deserializer=lambda m: json.loads(m.decode("utf-8"))
        )

        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

    def handle_message(self, msg):
        event = msg.get("event")

        if event == "turn_change":
            new_turnplayer = msg.get("turnplayer")
            self.current_turnplayer = new_turnplayer
            self.round_cnt = 0
            self.used_card_index = -1
            return
        elif event == "race_finished":
            new_finished_player = msg.get("finished")
            self.finished_players.append(new_finished_player)
            return

        player = msg.get("token")

        if event == "start":
            self.occupied = True
            self.occupied_by = player["id"]

            self.producer.send(self.segment_id, {"event": "run", "token": player})
            return

        if player:
            while self.current_turnplayer != player["id"]:
                time.sleep(0.5)

        if event == "scout":
            sender = msg.get("sender")
            if self.occupied and self.occupied_by == player["id"]:
                is_free = True
            else:
                is_free = not self.occupied

            steps_remaining = msg.get("steps_remaining")
            if logging == "logging":
                print(f"Segment {self.segment_id}: Received scout request from {sender}. Free? {is_free} Steps? {steps_remaining}")
            self.round_cnt = 0
            if is_free:
                if self.segment_type == "start-goal":
                    self.round_cnt = 1
                if self.segment_type == "caesar":
                    self.round_cnt = 1
                    if player["round"] < MAX_ROUNDS-1 and not player.get("visited_caesar", False):
                        for i in range(3):
                            if player["cards"][i] == player["cards"][player["orig_used_index"]] - steps_remaining:
                                if logging == "logging":
                                    print(f"Caesar available!")
                                self.next_free_segment_id = self.caesar_segment_id
                                self.used_card_index = i
                                steps_remaining = 0
                                break
                if steps_remaining > 0:
                    next_segment = self.get_next_free_segment(player, steps_remaining)
                    if next_segment:
                        self.producer.send(sender, value={"event": "availability", "segment": self.next_free_segment_id, "round_cnt": self.round_cnt, "used_index": self.used_card_index, "free": 1})
                    else:
                        self.producer.send(sender, value={"event": "availability", "segment": "test", "round_cnt": 0, "used_index": -1, "free": 2})
                else:
                    self.occupied = True
                    self.occupied_by = player["id"]
                    self.producer.send(sender, value={"event": "availability", "segment": self.segment_id, "round_cnt": self.round_cnt, "used_index": self.used_card_index, "free": 1})
            else:
                self.producer.send(sender, value={"event": "availability", "segment": "test", "round_cnt": 0, "used_index": -1, "free": 2})

        elif event == "availability":
            is_free = msg.get("free")
            self.next_free_segment_id = msg.get("segment")
            if msg.get("round_cnt") == 1:
                self.round_cnt = 1
            self.used_card_index = msg.get("used_index")
            self.next_is_free = is_free

        elif event == "run":
            if player["round"] >= MAX_ROUNDS:
                self.occupied = False
                self.occupied_by = 0
                print()
                print(f"Token {player['id']} completed {MAX_ROUNDS} rounds.")

                if player["id"] not in self.finished_players:
                    self.producer.send("turn_updates", value={"event": "race_finished", "finished": player["id"]})
                    time.sleep(self.segment_cnt * 0.5)
                    for i in range(len(self.finished_players)):
                        if player["id"] == self.finished_players[i]:
                            if player["visited_caesar"]:
                                print("Visited caesar!")
                                points = self.start_goals - i
                                print(f"Player {self.finished_players[i]} gets {points} points!")
                            else:
                                print("Didnt visit caesar... Lost!")
                                print("0 points!")

                if len(self.finished_players) < self.start_goals:
                    self.skip_turn(player)

                return

            if logging == "logging":
                print(f"Segment {self.segment_id} received event '{event}' with token: {player}")

            player["orig_used_index"] = 2
            next_segment = self.get_next_free_segment(player, player["cards"][2])

            if not next_segment:
                player["orig_used_index"] = 1
                next_segment = self.get_next_free_segment(player, player["cards"][1])

            if not next_segment:
                player["orig_used_index"] = 0
                next_segment = self.get_next_free_segment(player, player["cards"][0])

            if next_segment:
                if self.used_card_index != -1:
                    index = self.used_card_index
                else:
                    index = player["orig_used_index"]
                print()
                print(f"Player {player['id']} used card with number {player['cards'][index]}")
                cards = player["cards"]
                cards[index] = random.randint(1, 6)
                print(f"New card with number {cards[index]} added to hand")
                cards = sorted(cards)
                player["cards"] = cards
                self.used_card_index = -1
                self.move_token(self.next_free_segment_id, player)
                return

            print(f"Segment {self.segment_id}: No available paths. Skipping turn.")
            self.skip_turn(player)
            return

    def find_caesar_segment(self):
        if self.caesar_segment_id in self.next_segments:
            return self.caesar_segment_id
        return None

    def get_next_free_segment(self, player, steps):
        if player["round"] < MAX_ROUNDS-1 and not player.get("visited_caesar", False):
            caesar_segment = self.find_caesar_segment()
            if caesar_segment:
                found_segment = self.scout_segment(caesar_segment, player, steps)
                if found_segment:
                    return found_segment

        for next_segment in self.next_segments:
            found_segment = self.scout_segment(next_segment, player, steps)
            if found_segment:
                return found_segment

        return None

    def scout_segment(self, next_segment, player, steps):
        self.next_is_free = 0
        if logging == "logging":
            print(f"Segment {self.segment_id}: Scouting {next_segment} for availability...")
        self.producer.send(next_segment, value={"event": "scout", "sender": self.segment_id, "steps_remaining": steps - 1, "token": player})

        while self.next_is_free == 0:
            time.sleep(0.5)

        if self.next_is_free == 1:
            if logging == "logging":
                print(f"Segment {next_segment} is free!")
            player["round"] += self.round_cnt
            return self.next_free_segment_id
        elif self.next_is_free == 2:
            if logging == "logging":
                print(f"Segment {next_segment} is occupied. Trying another one...")
        return None

    def move_token(self, target_segment, player):
        if target_segment == self.caesar_segment_id:
            player["visited_caesar"] = True
        self.occupied = False
        self.occupied_by = 0
        next_turnplayer = (self.current_turnplayer % self.start_goals) + 1

        print(f"Player {player['id']} moved from segment {self.segment_id} to {target_segment} with {player}. Next turn: Player {next_turnplayer}")

        self.producer.send("turn_updates", value={"event": "turn_change", "turnplayer": next_turnplayer})
        print()
        print("-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------")
        print(f"[TURN UPDATE] Now it's Player {next_turnplayer}'s turn.")
        time.sleep(self.segment_cnt * 0.5)

        self.producer.send(target_segment, value={"event": "run", "token": player})

    def skip_turn(self, player):
        next_turnplayer = (self.current_turnplayer % self.start_goals) + 1
        print()
        if player["id"] in self.finished_players:
            print(f"[TURN SKIP] Player {player['id']} is finished. Now it's Player {next_turnplayer}'s turn.")
        else:
            print(f"[TURN SKIP] Segment {self.segment_id}: No move possible. Now it's Player {next_turnplayer}'s turn.")

        self.producer.send("turn_updates", value={"event": "turn_change", "turnplayer": next_turnplayer})
        print()
        print("-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------")
        print(f"[TURN UPDATE] Now it's Player {next_turnplayer}'s turn.")
        time.sleep(self.segment_cnt * 0.5)

        self.producer.send(self.segment_id, value={"event": "run", "token": player})


if __name__ == "__main__":
    segment_id = sys.argv[1]
    segment_type = sys.argv[2]
    next_segments = sys.argv[3].split(",")
    caesar_segment_id = sys.argv[4]
    start_goals = int(sys.argv[5])
    segment_cnt = int(sys.argv[6])
    logging = sys.argv[7]

    segment = Segment(segment_id, segment_type, next_segments, caesar_segment_id, start_goals, segment_cnt, logging)

    try:
        for msg in segment.consumer:
            thread = threading.Thread(target=segment.handle_message, args=(msg.value,))
            thread.start()
    except KeyboardInterrupt:
        print("\n[INFO] Exiting gracefully...")
        sys.exit(0)
