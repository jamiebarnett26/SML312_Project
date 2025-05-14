import time
import requests
import pandas as pd
import os
import json
from requests.auth import HTTPBasicAuth


BASE_URL = "https://api.followupboss.com/v1"
HEADERS = {

}
AUTH = HTTPBasicAuth('', '')

EVENT_TYPES = ["textMessages", "tasks"]
FILE_NAMES = {event: f"{event}.csv" for event in EVENT_TYPES}
PROGRESS_FILE = 'progress.json'
PERSON_IDS_FILE = 'people.csv'

try:
    target_person_ids = set(pd.read_csv(PERSON_IDS_FILE)['id'].astype(str))
except Exception as e:
    raise RuntimeError("error")


def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                print("error")
    return {}

def save_progress(progress):
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2)
        f.flush()
        os.fsync(f.fileno())

    display_progress = {
        k: len(v) if isinstance(v, list) else 0
        for k, v in progress.items()
    }

def load_existing_ids(event_type):
    csv_file = FILE_NAMES[event_type]
    if os.path.exists(csv_file):
        try:
            df = pd.read_csv(csv_file, usecols=["id"])
            return set(df["id"].astype(str))
        except Exception as e:
            print("error")
    return set()

def fetch_event_data(event_type, seen_ids, processed_ids, progress):
    for person_id in target_person_ids:
        if person_id in processed_ids:
            continue

        url = f"{BASE_URL}/{event_type}?limit=100&personId={person_id}"
        collected_data = []

        while url:
            response = requests.get(url, headers=HEADERS, auth=AUTH)

            if response.status_code == 200:
                response_data = response.json()
                items = response_data.get('textmessages', [])

                new_data = []
                for item in items:
                    new_data.append(item)

                store_data(new_data, event_type)

                next_link = response_data.get("_metadata", {}).get("nextLink")
                time.sleep(0.001)

                if next_link and next_link != url:
                    url = next_link
                else:
                    break

            elif response.status_code == 429:
                print("Rate limit")
                time.sleep(60)
            elif response.status_code == 401:
                raise Exception("Unauthorized")
            else:
                return None

        if collected_data:
            store_data(collected_data, event_type)

        processed_ids.add(person_id)
        progress[event_type] = list(processed_ids)
        remaining = len(target_person_ids - processed_ids)
        save_progress(progress)



def store_data(data, event_type):
    if not data:
        return
    df = pd.DataFrame(data)
    csv_file = FILE_NAMES[event_type]
    df.to_csv(csv_file, mode='a', header=not os.path.exists(csv_file), index=False)


def main():
    progress = load_progress()

    for event_type in EVENT_TYPES:
        seen_ids = load_existing_ids(event_type)
        processed_ids = set(progress.get(event_type) or [])

        data = fetch_event_data(event_type, seen_ids, processed_ids, progress)

        if data:
            store_data(data, event_type)

        time.sleep(0.01)

    save_progress(progress)

if __name__ == "__main__":
    main()
