#!/usr/bin/env python3
import sys
import json
import random

def generate_random_array(num_tracks, num_seg):
    options = ["normal", "wall-down", "bottleneck"]

    segments = []
    segment = []

    for _ in range(0,num_tracks):
        segment.append("start-goal")
    segments.append(segment)

    for _ in range(1,num_seg):
        segment = []

        # Choose the first element
        first_choice = random.choice(options)
        segment.append(first_choice)

        if first_choice == "bottleneck":
            # If first is "bottleneck", fill the rest with "bottleneck"
            segment.extend(["bottleneck"] * (num_tracks - 1))
        else:
            # Otherwise, ensure bottleneck is not in the rest
            track_options = ["normal", "wall-down"]
            segment.extend(random.choices(track_options, k=num_tracks - 2))

            # Ensure the last element isn't "wall"
            segment.append("normal")

        segments.append(segment)
    return segments

def generate_next_segments(segment, track, num_segments, segment_types, num_tracks):
    segment_type_up = None
    if track == 0:
        segment_type = "caesar"
    else:
        segment_type = segment_types[segment][track - 1]

    next_segment = (segment + 1) % num_segments

    next_segment_type_up = None
    next_segment_type_forward = segment_types[next_segment][track - 1]

    segments = []

    if track > 1:
        segment_type_up = segment_types[segment][track - 2]
        next_segment_type_up = segment_types[next_segment][track - 2]

    if track == 1 and next_segment_type_forward == "start-goal":
        segments.append("segment-0-0")

    if segment_type == "caesar":
        segments.append("segment-1-1")
        return segments

    if next_segment_type_forward == "bottleneck":
        segments.append(f"segment-1-{next_segment}")
        return segments

    if segment_type == "bottleneck":
        for t in range(1, num_tracks + 1):
            segments.append(f"segment-{t}-{next_segment}")
        return segments

    if track > 1:
        if not segment_type_up == "wall-down" or not next_segment_type_up == "wall-down":
            segments.append(f"segment-{track-1}-{next_segment}")

    segments.append(f"segment-{track}-{next_segment}")

    if track < num_tracks:
        if not segment_type == "wall-down" or not next_segment_type_forward == "wall-down":
            segments.append(f"segment-{track+1}-{next_segment}")

    return segments


def generate_tracks(num_tracks: int, length_of_track: int):
    """
    Generates a data structure with 'num_tracks' circular tracks.
    Each track has exactly 'length_of_track' segments:
      - 1 segment: 'segment-t-0'
      - (length_of_track - 1) segments: 'segment-t-c'
    Returns a Python dict that can be serialized to JSON.
    """
    all_tracks = []
    track_segment_types = generate_random_array(num_tracks, length_of_track)

    track_id = str(0)
    seg_id = "segment-0-0"
    segments = []
    segment = {
        "segmentId": seg_id,
        "type": "caesar",
        "nextSegments": generate_next_segments(0, 0, length_of_track, track_segment_types, num_tracks)
    }
    segments.append(segment)

    track_definition = {
        "trackId": track_id,
        "segments": segments
    }
    all_tracks.append(track_definition)

    for t in range(1, num_tracks + 1):
        track_id = str(t)
        segments = []

        # First segment: start-and-goal-t
        start_segment_id = f"segment-{t}-0"

        start_segment = {
            "segmentId": start_segment_id,
            "type": "start-goal",
            "nextSegments": generate_next_segments(0, t, length_of_track, track_segment_types, num_tracks)
        }
        segments.append(start_segment)

        # Create normal segments: segment-t-c for c in [1..(L-1)]
        for c in range(1, length_of_track):
            if track_segment_types[c][0] == "bottleneck" and t != 1:
                continue
            seg_id = f"segment-{t}-{c}"

            segment = {
                "segmentId": seg_id,
                "type": track_segment_types[c][t-1],
                "nextSegments": generate_next_segments(c, t, length_of_track, track_segment_types, num_tracks)
            }
            segments.append(segment)


        track_definition = {
            "trackId": track_id,
            "segments": segments
        }
        all_tracks.append(track_definition)



    return {"tracks": all_tracks}


def main():
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} <num_tracks> <length_of_track> <output_file>")
        sys.exit(1)

    num_tracks = int(sys.argv[1])
    length_of_track = int(sys.argv[2])
    output_file = sys.argv[3]

    if length_of_track < 6:
        print(f"length_of_track: {length_of_track} smaller than 6")
        sys.exit(1)

    tracks_data = generate_tracks(num_tracks, length_of_track)

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(tracks_data, f, indent=2)
        f.write('\n')
    print(f"Successfully generated {num_tracks} track(s) of length {length_of_track} into '{output_file}'")

if __name__ == "__main__":
    main()

