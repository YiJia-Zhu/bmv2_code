#!/usr/bin/env python3
import json
import sys
import requests

import os
import boto3
from smart_open import open
from datasets import load_dataset


session = boto3.Session(
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"])
s3 = session.client("s3")


def download_contents(blob_id, src_encoding):
    print(f"Downloading {blob_id} ...", end=" ")
    s3_url = f"s3://softwareheritage/content/{blob_id}"

    with open(s3_url, "rb", compression=".gz", transport_params={"client": s3}) as fin:
        content = fin.read().decode(src_encoding)

    print("Done")
    return content


def process_json(js, id_):
    blob_id = js["blob_id"]
    src_encoding = js["src_encoding"]

    content = download_contents(blob_id, src_encoding)
    # Append the new entry with the stringified content
    new_entry = {
        "repo_name": "dummy_name",  # Modify as needed
        "repo_path": "dummy_path",  # Modify as needed
        "id": id_,  # Adjust as needed
        "content": content
    }

    return new_entry


def main():
    try:
        with open(sys.argv[2], 'r') as json_file:
            data = json.load(json_file)
    except FileNotFoundError:
        data = []  # Start a new list if the file doesn't exist

    with open(sys.argv[1]) as fin:
        lines = fin.readlines()

    repos = set()
    for line in lines:
        js = json.loads(line)
        new_entry = process_json(js, len(data))
        data.append(new_entry)

    print("\n".join(repos))

    # Save the updated JSON data back to p416dataset.json
    with open(sys.argv[3], 'w') as json_file:
        json.dump(data, json_file, indent=4)


if __name__ == "__main__":
    main()
