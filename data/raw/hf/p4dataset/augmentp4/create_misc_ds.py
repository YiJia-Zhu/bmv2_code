#!/usr/bin/env python3
import os
import json
import logging
import argparse
from pathlib import Path


directories = [
        "other",
        "rfcs",
        "discussions",
        "papers"
        ]


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
    )


def process_file(file_path):
    try:
        with open(file_path, "r") as file:
            return file.read()
    except Exception as e:
        logging.error(f"Failed to process file {file_path}: {e}")
        return None


def create_misc_ds(augmentdir, ds):
    lines = []
    id_ = 0

    for directory in directories:
        directory_path = Path(os.path.join(augmentdir, directory))
        if not directory_path.exists() or not directory_path.is_dir():
            logging.warning(f"Skipping invalid or non-existent directory_path: {directory_path}")
            continue

        for filename in os.listdir(directory_path):
            file_path = directory_path / filename
            if file_path.is_file():
                content = process_file(file_path)
                if content is None:
                    continue

                new_entry = {
                    "repo_name": "__dummy_name",
                    "repo_path": "__dummy_path",
                    "id": id_,
                    "content": content
                }
                id_ += 1
                lines.append(json.dumps(new_entry) + "\n")

    try:
        with open(ds, "w") as fout:
            fout.writelines(lines)
    except Exception as e:
        logging.error(f"Failed to write dataset to {ds}: {e}")


def main():
    parser = argparse.ArgumentParser(description="Process files from directories and create a JSON dataset.")
    parser.add_argument("--dir", required=True, help="Augmentation directory")
    parser.add_argument("-o", "--output", required=True, help="Output JSON file path")

    setup_logging()
    args = parser.parse_args()
    create_misc_ds(args.dir, args.output)


if __name__ == "__main__":
    main()
