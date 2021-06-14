from pathlib import Path

raw_root = Path("/mnt/data/ubermedia-raw")
raw_path = raw_root / "2021-06-11-raw.tsv"
parts_root = raw_root / "parts"

parts_root.mkdir(exist_ok=True)
