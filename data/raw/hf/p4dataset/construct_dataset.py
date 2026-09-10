#!/usr/bin/env python3
import argparse
import json
import logging
import multiprocessing as mp
import os
import traceback

import chardet
from tqdm import tqdm

import helpers
from helpers import getLogger, setup_logging


def read_file_safely(filepath):
    """Reads a file safely, attempting multiple encodings if necessary.

    Args:
        filepath (str): Path to the file.

    Returns:
        str: Decoded file content.
    """
    try:
        with open(filepath, "rb") as fin:
            raw_data = fin.read()

        detected = chardet.detect(raw_data)
        encoding = detected.get("encoding", "utf-8")

        try:
            return raw_data.decode(encoding)
        except UnicodeDecodeError:
            logging.warning("Failed to decode %s with detected encoding %s", filepath, encoding)
            return ""

    except Exception as e:
        logging.warning("Error reading file: %s", filepath)
        logging.warning("Error details: %s", "".join(traceback.format_exception(e)))
        return None


def get_all_files(directory):
    """Get all files in a directory.

    Args:
        directory (str): The directory to search.

    Returns:
        list: A list of file paths.

    """
    files = []
    for root, _, filenames in os.walk(directory):
        for filename in filenames:
            files.append(os.path.join(root, filename))

    return files


def process_directory(directory):
    """Process files in a directory and generate JSON entries.

    Args:
        directory (str): The root directory to process.

    Returns:
        list: A list of JSON strings representing processed files.

    """
    id_ = 0
    lines = []
    logging.info("Starting to process directory: %s", directory)

    tasks = get_all_files(directory)
    for root, _, files in os.walk(directory):
        for filename in files:
            filepath = os.path.join(root, filename)
            try:
                user, repo, path = filepath.split(os.sep, 2)[2].split(os.sep, 2)
                repo_name = os.path.join(user, repo)
                path = "/" + path  # match the format used in the Stack v2

                # with open(filepath, "rt") as fin:
                    # content = fin.read()
                content = read_file_safely(filepath)
                if content is None:
                    continue

                entry = {
                    "repo_name": repo_name,
                    "repo_path": path,
                    "id": id_,
                    "content": content
                }
                lines.append(json.dumps(entry) + "\n")
                id_ += 1

            except Exception as e:
                logging.warning("Failed to process file: %s", filepath)
                estr = "".join(traceback.format_exception(e))
                logging.warning("Error details: %s", estr)

    logging.info("Finished processing directory: %s", directory)
    return lines


def process_file(filepath):
    setup_logging()
    logger = getLogger()

    try:
        user, repo, path = filepath.split(os.sep, 2)[2].split(os.sep, 2)
        repo_name = os.path.join(user, repo)
        path = "/" + path  # match the format used in the Stack v2

        content = read_file_safely(filepath)
        if content is None:
            logger.warning("Failed to read file: %s", filepath)
            return None

        entry = {
            "repo_name": repo_name,
            "repo_path": path,
            "content": content
        }
        return entry
    except Exception as e:
        logger.warning("Failed to process file: %s", filepath)
        estr = "".join(traceback.format_exception(e))
        logger.warning("Error details: %s", estr)


def construct_dataset(srcdir, dspath):
    """Construct a dataset from files in a directory.

    Args:
        srcdir (str): The directory to process.
        dspath (str): The path to the output dataset file.

    """
    logging.info("Starting dataset construction from directory: %s", srcdir)
    tasks = get_all_files(srcdir)
    id_ = 0
    results = []
    with mp.Pool(processes=mp.cpu_count()) as pool:
        with tqdm(total=len(tasks), desc="Processing files",
                  unit="file") as pbar:
            for result in pool.imap_unordered(process_file, tasks):
                if result is None:
                    continue

                result["id"] = id_
                id_ += 1
                results.append(result)
                pbar.update()

    lines = [json.dumps(entry) + "\n" for entry in results]
    with open(dspath, "wt") as fout:
        fout.writelines(lines)

    logging.info("Dataset successfully written to: %s", dspath)


def main():
    parser = argparse.ArgumentParser(description="Process files in a directory to construct a dataset.")
    parser.add_argument("srcdir", type=str, help="Source directory to process.")
    parser.add_argument("dspath", type=str, help="Path to the output dataset file.")
    parser.add_argument("--num_processes", type=int, default=mp.cpu_count(),
                        help="Number of processes to use (default: number of CPU cores)")
    parser.add_argument("--logdir", type=str, default=None,
                        help="Name of the log directory (default: %(default)s)")

    args = parser.parse_args()

    construct_dataset(args.srcdir, args.dspath)


if __name__ == "__main__":
    main()
