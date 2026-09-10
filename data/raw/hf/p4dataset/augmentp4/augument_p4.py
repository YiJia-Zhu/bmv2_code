#!/usr/bin/env python3
import json
import os
import sys

# List of directories containing the files
directories = [
        'other',
        'rfcs',
        'discussions',
        'papers'
        ]


# Function to process each file
def process_file(file_path):
    with open(file_path, 'r') as file:
        content = file.read()
    return content


def main():
    # Load the existing JSON data from p416dataset.json
    with open(sys.argv[1], 'r') as fin:
        lines = fin.readlines()

    counter = 0
    # Iterate over each directory in the list
    for directory_path in directories:
        for filename in os.listdir(directory_path):
            counter += 1
            file_path = os.path.join(directory_path, filename)
            # Check if it's a file
            if os.path.isfile(file_path):
                # Process the file and get its content
                content = process_file(file_path)
                # Append the new entry with the stringified content
                new_entry = {
                    "repo_name": "dummy_name",  # Modify as needed
                    "repo_path": "dummy_path",  # Modify as needed
                    "id": len(lines),  # Adjust as needed
                    "content": content
                }
                lines.append(json.dumps(new_entry) + "\n")

    print("Counter: ", counter)
    print(len(lines))
    # Save the updated JSON data back to p416dataset.json
    with open(sys.argv[2], 'w') as fout:
        fout.writelines(lines)


if __name__ == "__main__":
    main()
