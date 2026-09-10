#!/usr/bin/env python3
import json
import re
import sys
import argparse
import logging

# Constants
MIN_SIZE_THRESHOLD = 10
MAX_SIZE_THRESHOLD = 300000

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO
)

def bad_size(contents):

    data = contents.encode("utf-8")
    return len(data) not in range(MIN_SIZE_THRESHOLD, MAX_SIZE_THRESHOLD)


def maybe_p4(contents):
    patterns = [
        r"action",
        r"table",
        r"control",
        r"parser",
        r"include",
        r"#define",
        r"(var)?bit<\d+>",
    ]

    return any(re.search(pattern, contents) for pattern in patterns)


def is_blacklisted(entry, blacklist):
    name = entry["repo_name"] + entry["repo_path"]
    if name in blacklist:
        return True
    contents = entry["content"]
    return "Invoking preprocessor" in contents.split("\n")[0]


def delete_non_ascii_comments(contents):
    single_line_comment_re = r'//.*?$'
    multi_line_comment_re = r'/\*.*?\*/'

    def contains_non_ascii(s):
        return any(ord(c) > 127 for c in s)

    contents = re.sub(single_line_comment_re, '', contents, flags=re.MULTILINE | re.DOTALL)

    def remove_non_ascii_multiline_comments(match):
        return '' if contains_non_ascii(match.group(0)) else match.group(0)

    return re.sub(multi_line_comment_re, remove_non_ascii_multiline_comments, contents, flags=re.DOTALL)


def cleanup_line_endings(contents):
    return contents.replace('\r', '')


def cleanup(contents):
    contents = delete_non_ascii_comments(contents)
    contents = cleanup_line_endings(contents)
    return contents

def filter_bad_files(src_file, dst_file, blacklist=None):
    try:
        with open(src_file, "rt", encoding="utf-8") as fin:
            lines = fin.readlines()
    except Exception as e:
        logging.error(f"Error reading source file {src_file}: {e}")
        sys.exit(1)

    result = []
    for line in lines:
        try:
            js = json.loads(line)
            contents = js.get("content", "")
            if not contents:
                logging.warning("Skipping entry with no content")
                continue

            isp4 = maybe_p4(contents)
            badsize = bad_size(contents)
            blacklisted = is_blacklisted(js, blacklist) if blacklist else False
            if isp4 and not badsize and not blacklisted:
                js["content"] = cleanup(contents)
                result.append(json.dumps(js) + "\n")
        except json.JSONDecodeError:
            logging.warning("Skipping malformed JSON entry")
        except KeyError as e:
            logging.warning(f"Missing key {e} in JSON entry, skipping.")

    try:
        with open(dst_file, "wt", encoding="utf-8") as fout:
            fout.writelines(result)
        logging.info(f"Filtered data successfully written to {dst_file}")
    except Exception as e:
        logging.error(f"Error writing to destination file {dst_file}: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Filter bad P4 files based "
                                     "on content and size constraints.")
    parser.add_argument("src", help="Path to the source dataset (JSON Lines "
                        "format)")
    parser.add_argument("dst", help="Path to the destination dataset")
    parser.add_argument("--blacklist", help="Path to a file containing "
                        "blacklisted P4 files")
    args = parser.parse_args()

    logging.info(f"Processing file: {args.src}")

    blacklist = None
    if args.blacklist:
        blacklist = set()
        with open(args.blacklist, "rt") as fin:
            for line in fin:
                blacklist.add(line.strip())

    filter_bad_files(args.src, args.dst, blacklist)
    logging.info("Processing complete.")


if __name__ == "__main__":
    main()
