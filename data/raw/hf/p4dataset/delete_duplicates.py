#!/usr/bin/env python3
import hashlib
import os
import sys


def compute_sha1(filepath):
    """Compute the SHA1 hash of the file at the given filepath."""
    sha1 = hashlib.sha1()
    with open(filepath, 'rb') as f:
        while True:
            data = f.read(65536)  # Read in 64kB chunks
            if not data:
                break
            sha1.update(data)
    return sha1.hexdigest()

def process_directory(directory, file_hash_dict):
    """Recursively process files in the directory and update the hash dictionary."""
    for root, dirs, files in os.walk(directory):
        for filename in files:
            filepath = os.path.join(root, filename)
            file_hash = compute_sha1(filepath)
            if file_hash in file_hash_dict:
                file_hash_dict[file_hash].append(filepath)
            else:
                file_hash_dict[file_hash] = [filepath]

def delete_duplicates(file_hash_dict):
    """Delete file paths of duplicate files."""
    for file_list in file_hash_dict.values():
        if len(file_list) > 1:
            for file in file_list[1:]:
                print(f"Removing {file} ...")
                os.remove(file)

# Main execution
file_hashes = {}
directory_to_search = sys.argv[1]  # Replace with your directory path
process_directory(directory_to_search, file_hashes)
delete_duplicates(file_hashes)
