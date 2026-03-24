import json
import os
from datetime import datetime

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def get_current_timestamp():
    return datetime.now().strftime("%Y-%m-%dT%H-%M-%S")

def save_json(data, output_dir, prefix):
    ensure_dir(output_dir)

    timestamp = get_current_timestamp()
    file_path = os.path.join(output_dir, f"{prefix}_{timestamp}.json")

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    return file_path