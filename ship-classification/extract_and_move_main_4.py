"""
Script to:
- Extract only WAV files associated with ferry, pleasure, tug, and cargo
- Rename the files to a standard format
- Move them to a target folder
- Create two CSVs:
    (1) new filenames and ship types
    (2) old filenames mapped to new filenames
"""

import os
import shutil
import csv
from pathlib import Path
import re


# === Configuration ===
TYPE_MAP = {
    "typecargo_60": "ferry",
    "typecargo_37": "pleasure_craft",
    "typecargo_52": "tug",
    "typecargo_70": "cargo",
    "typecargo_71": "cargo",
    "typecargo_79": "cargo",
    "typecargo_73": "cargo",
}

NEW_TYPE_MAP = {
    "37": "1",
    "52": "2",
    "60": "3",
    "70": "4a",
    "71": "4b",
    "73": "4c",
    "79": "4d",
}

OG_FOLDER = Path("/Users/catherinebertozzi/hackathon-datasets/ship-classification/data/oceanship")
TARGET_FOLDER = Path("/Users/catherinebertozzi/hackathon-datasets/ship-classification/wav-files")
TARGET_FOLDER.mkdir(exist_ok=True, parents=True)

CSV_PATH = TARGET_FOLDER / "wav_files.csv"
COLLISION_CSV = TARGET_FOLDER / "collisions.csv"
MAPPING_CSV = TARGET_FOLDER / "filename_mapping.csv"


# === Tracking containers ===
csv_rows = []
collision_rows = []
mapping_rows = []
unmatched = []

# Counters
total_files = len(list(OG_FOLDER.iterdir()))
moved_files = 0
collisions = 0
main_4 = 0


# === Processing ===
for fname in os.listdir(OG_FOLDER):
    if not fname.endswith(".wav"):
        continue

    for code, ship_type in TYPE_MAP.items():
        if code not in fname:
            continue

        main_4 += 1
        match = re.match(r"(\d{8}T\d{6}\.\d+Z).*typecargo_(\d+)(?:_(\d+))?\.wav$", fname)

        if not match:
            unmatched.append(fname)
            break

        timestamp, type_num, segment = match.groups()
        segment = segment or "0"

        new_name = f"{timestamp}_class_{NEW_TYPE_MAP[type_num]}_seg_{segment}.wav"
        src_path = OG_FOLDER / fname
        dst_path = TARGET_FOLDER / new_name

        # --- Collision check ---
        if dst_path.exists():
            print(f"⚠️ Collision detected: {fname} → {new_name}")
            collision_rows.append([fname, new_name, str(dst_path)])
            collisions += 1
            break

        # Copy and record
        shutil.copy2(src_path, dst_path)
        csv_rows.append([ship_type, new_name])
        mapping_rows.append([fname, new_name])
        moved_files += 1
        break  # stop checking other codes once matched


# === Write CSVs ===
with CSV_PATH.open("w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["Type", "Filename"])
    writer.writerows(csv_rows)

if collision_rows:
    with COLLISION_CSV.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["original_filename", "target_filename", "target_path"])
        writer.writerows(collision_rows)
    print(f"\n{collisions} filename collisions logged to: {COLLISION_CSV}")

with MAPPING_CSV.open("w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["original_filename", "new_filename"])
    writer.writerows(mapping_rows)

print(f"CSV mapping original → new filenames created at: {MAPPING_CSV}")


# === Summary ===
print("\n=== Summary ===")
print(f"Total files in original folder: {total_files}")
print(f"Total files main 4: {main_4}")
print(f"Total files moved/renamed: {moved_files}")
print(f"Total collisions: {collisions}")
print(f"Unmatched files: {len(unmatched)} ({unmatched[:5]}...)")  # show preview

# Count target folder files (excluding CSVs)
num_files = sum(
    1 for f in TARGET_FOLDER.iterdir()
    if f.is_file() and f.suffix == ".wav"
)
print(f"Total WAV files in target folder: {num_files}")
