import os
import csv
import wave
import numpy as np

folder = "/Users/catherinebertozzi/hackathon-datasets/ship-classification/data/oceanship"

type_map = {
    "typecargo_60": "Passenger",
    "typecargo_37": "Pleasure Craft",
    "typecargo_52": "Tug",
    "typecargo_70": "Cargo",
    "typecargo_71": "Cargo",
    "typecargo_79": "Cargo",
    "typecargo_73": "Cargo"
}

rows = []

for f in os.listdir(folder):
    if not f.endswith(".wav"):
        continue
    fpath = os.path.join(folder, f)
    for key, ship_type in type_map.items():
        if key in f:
            with wave.open(fpath, 'r') as wf:
                frames = wf.getnframes()
                rate = wf.getframerate()
                duration = frames / float(rate)
            rows.append({
                "filename": f,
                "type": ship_type,
                "duration": duration
            })
            break  # stop after first match

# Save to CSV
out_csv = os.path.join(folder, "filtered_filenames_with_duration.csv")
with open(out_csv, "w", newline="") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=["filename", "type", "duration"])
    writer.writeheader()
    writer.writerows(rows)

# Summary stats
durations = np.array([r["duration"] for r in rows])
avg = durations.mean()
min_d = durations.min()
max_d = durations.max()
near_5s = ((durations > 4.5) & (durations < 5.5)).sum() / len(durations) * 100

print(f"✅ Saved {len(rows)} entries to {out_csv}")
print(f"Average length: {avg:.2f}s")
print(f"Min: {min_d:.2f}s | Max: {max_d:.2f}s")
print(f"{near_5s:.1f}% of clips are between 4.5–5.5 seconds")
