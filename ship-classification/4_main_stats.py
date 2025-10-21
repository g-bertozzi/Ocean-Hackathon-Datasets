import pandas as pd
import numpy as np

# Path to your CSV file
csv_path = "/Users/catherinebertozzi/hackathon-datasets/ship-classification/filtered_filenames_with_duration.csv"  # change if needed

# Load the CSV
df = pd.read_csv(csv_path)

# Basic duration array
durations = df["duration"].values

# Summary statistics
summary = {
    "count": len(durations),
    "mean": np.mean(durations),
    "median": np.median(durations),
    "std": np.std(durations, ddof=1),
    "min": np.min(durations),
    "max": np.max(durations),
    "25%": np.percentile(durations, 25),
    "75%": np.percentile(durations, 75),
    "90%": np.percentile(durations, 90),
    "95%": np.percentile(durations, 95),
    "99%": np.percentile(durations, 99),
}

print("=== Overall Duration Summary ===")
for k, v in summary.items():
    print(f"{k:>6}: {v:.3f}")

# Percent of clips near target length (e.g., around 5 seconds)
target = 5.0
tolerance = 0.5
within_range = ((durations >= target - tolerance) & (durations <= target + tolerance)).sum()
pct_within = within_range / len(durations) * 100
print(f"\n{pct_within:.2f}% of files are between {target - tolerance}–{target + tolerance} seconds")

# Optional: per-type summaries if 'type' column exists
if "type" in df.columns:
    print("\n=== Per-Type Duration Summary ===")
    for ship_type, group in df.groupby("type"):
        arr = group["duration"].values
        mean = np.mean(arr)
        median = np.median(arr)
        pct_near = ((arr >= target - tolerance) & (arr <= target + tolerance)).sum() / len(arr) * 100
        print(f"{ship_type:15s}  n={len(arr):5d}  mean={mean:5.3f}s  median={median:5.3f}s  ~5s={pct_near:5.1f}%")


        total = group["duration"].sum()
        hours = total / 3600  # convert seconds to hours
        print(f"{ship_type:15s}  total={total:8.2f}s  ({hours:.2f} hours)")


total_duration = durations.sum()
print(f"\nTotal duration of all audio files: {total_duration:.3f} seconds ({total_duration/3600:.2f} hours)")
