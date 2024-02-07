import json
from pathlib import Path


def start():
    """
    Start the API generation process
    """
    out_data = Path("out.json").read_text(encoding="utf-8")
    data = json.loads(out_data)

    chunk_size = 50
    split_lists = []
    for i in range(0, len(out_data), chunk_size):
        split_lists.append(out_data[i : i + chunk_size])

    print(split_lists)
