import pandas as pd
import re

# Load csv
df = pd.read_csv("/Users/catherinebertozzi/hackathon-datasets/ship-classification/data/oceanship/label.csv")

# Extract the "typecargo_xx" part
df["code"] = df["path"].str.extract(r"(typecargo_\d+)")

# Build a dictionary of unique mappings-- many to one relationship?
mapping = df.drop_duplicates(subset=["code", "label"]).set_index("code")["label"].to_dict()

print("Found mappings:")
for code, label in mapping.items():
    print(f"{code} -> {label}")
