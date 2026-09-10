#!/usr/bin/env python3
import json
import argparse
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

gid = 0


def add_dentry(d, line):
    """Add an entry to the dictionary by parsing a JSON line."""
    global gid
    try:
        jl = json.loads(line)
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON: {e}")
        raise

    key = jl["repo_name"] + jl["repo_path"]
    if jl["repo_name"] == "__dummy_repo" or jl["repo_path"] == "__dummy_path":
        key = (gid, key)
        gid += 1

    d[key] = jl


def update_ds(target, src, result):
    """Update the dataset based on the source and write the results."""
    try:
        with open(target, "rt") as fin:
            target_lines = fin.readlines()
    except FileNotFoundError:
        logger.error(f"Target file '{target}' not found.")
        raise
    except IOError as e:
        logger.error(f"Error reading target file '{target}': {e}")
        raise

    target_d = {}
    for line in target_lines:
        add_dentry(target_d, line)

    try:
        with open(src, "rt") as fin:
            src_lines = fin.readlines()
    except FileNotFoundError:
        logger.error(f"Source file '{src}' not found.")
        raise
    except IOError as e:
        logger.error(f"Error reading source file '{src}': {e}")
        raise

    src_d = {}
    for line in src_lines:
        add_dentry(src_d, line)

    # Merge dictionaries
    for k, v in src_d.items():
        if k not in target_d:
            target_d[k] = v

    # Write the result
    try:
        with open(result, 'w') as fout:
            for entry in target_d.values():
                fout.write(json.dumps(entry) + '\n')
    except IOError as e:
        logger.error(f"Error writing to result file '{result}': {e}")
        raise


def main():
    parser = argparse.ArgumentParser(description="Update a dataset by merging with a source dataset.")
    parser.add_argument("target", help="Path to the target file.")
    parser.add_argument("src", help="Path to the source file.")
    parser.add_argument("result", help="Path to the output result file.")

    args = parser.parse_args()

    try:
        update_ds(args.target, args.src, args.result)
        logger.info(f"Successfully updated dataset and saved to '{args.result}'.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        exit(1)


if __name__ == "__main__":
    main()
