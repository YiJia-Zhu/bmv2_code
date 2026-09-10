#!/usr/bin/env python3
import argparse
import logging
import os
import re
import shutil
import sys

# Create a top-level logger for this module.
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler(sys.stderr)
logger.addHandler(stream_handler)


def copy_files_with_extension(src_dir, dest_dir, extension):
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    for root, dirs, files in os.walk(src_dir):
        for file in files:
            if re.match(rf".*\.{extension}$", file, re.I) and \
               os.path.isfile(os.path.join(root, file)):
                src_path = os.path.join(root, file)
                relative_path = os.path.relpath(src_path, src_dir)
                dest_path = os.path.join(dest_dir, relative_path)

                dest_dir_path = os.path.dirname(dest_path)
                if not os.path.exists(dest_dir_path):
                    os.makedirs(dest_dir_path)

                shutil.copy2(src_path, dest_path)
                logger.info(f"Copied: {src_path} to {dest_path}")


def main():
    parser = argparse.ArgumentParser(description="Copy files with a specific extension from one directory to another.")
    parser.add_argument("src_dir", type=str, help="Source directory to copy files from.")
    parser.add_argument("dest_dir", type=str, help="Destination directory to copy files to.")
    parser.add_argument("-e", "--extension", type=str, default="p4", help="File extension to filter by (default: 'p4').")

    args = parser.parse_args()

    # Validate source directory
    if not os.path.isdir(args.src_dir):
        logger.error(f"Source directory does not exist: {args.src_dir}")
        exit(1)

    # Call the function with parsed arguments
    copy_files_with_extension(args.src_dir, args.dest_dir, args.extension)


if __name__ == "__main__":
    main()
