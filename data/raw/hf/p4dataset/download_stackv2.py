#!/usr/bin/env python3
import json
import logging
import argparse
import os
import boto3
from smart_open import open

s3 = None

def setup_logging():
    """Sets up logging for the script."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def download_contents(blob_id, src_encoding):
    """Downloads and decodes content from S3.

    Args:
        blob_id (str): The S3 blob ID.
        src_encoding (str): The source encoding of the content.

    Returns:
        str: Decoded content.
    """
    logging.info(f"Downloading {blob_id} ...")
    s3_url = f"s3://softwareheritage/content/{blob_id}"

    try:
        with open(s3_url, "rb", compression=".gz", transport_params={"client": s3}) as fin:
            content = fin.read().decode(src_encoding)
        logging.info(f"Successfully downloaded {blob_id}.")
    except Exception as e:
        logging.error(f"Failed to download {blob_id}: {e}")
        raise

    return content

def process_json(js, id_):
    """Processes a single JSON object, downloading its associated content.

    Args:
        js (dict): JSON object containing blob details.
        id_ (int): Unique ID for the processed entry.

    Returns:
        dict: New JSON entry with downloaded content.
    """
    blob_id = js["blob_id"]
    src_encoding = js["src_encoding"]

    content = download_contents(blob_id, src_encoding)
    new_entry = {
        "repo_name": js["repo_name"],
        "repo_path": js["path"],
        "id": id_,
        "content": content
    }
    return new_entry

def download_stack_files(stackv2linksds, stackds):
    """Downloads and processes stack files, saving the results.

    Args:
        stackv2linksds (str): Path to the input file containing JSON lines.
        stackds (str): Path to the output file for processed JSON lines.
    """
    logging.info("Initializing S3 session...")
    session = boto3.Session(
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY")
    )
    global s3
    s3 = session.client("s3")

    try:
        with open(stackv2linksds) as fin:
            lines = fin.readlines()
        logging.info(f"Loaded {len(lines)} entries from {stackv2linksds}.")
    except Exception as e:
        logging.error(f"Failed to read input file {stackv2linksds}: {e}")
        raise

    data = []
    for id_, line in enumerate(lines):
        try:
            js = json.loads(line)
            new_entry = process_json(js, id_)
            data.append(json.dumps(new_entry) + "\n")
        except Exception as e:
            logging.error(f"Error processing line {id_}: {e}")

    try:
        with open(stackds, 'w') as fout:
            fout.writelines(data)
        logging.info(f"Successfully wrote processed data to {stackds}.")
    except Exception as e:
        logging.error(f"Failed to write output file {stackds}: {e}")
        raise

def main():
    """Main function to parse arguments and execute the script."""
    parser = argparse.ArgumentParser(description="Download and process stack files.")
    parser.add_argument("input_file", help="Path to the input file containing JSON lines.")
    parser.add_argument("output_file", help="Path to the output file for processed JSON lines.")

    args = parser.parse_args()

    setup_logging()

    try:
        download_stack_files(args.input_file, args.output_file)
    except Exception as e:
        logging.critical(f"Script failed with error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
