#!/usr/bin/env python3
import json
import sys
import re

def is_p414(contents):
    # Define the regex patterns
    patterns = [
        r"reads\s+{",
        r"fields\s+{",
        r"parser\s+start\s+{",
        r"control\s+ingress\s+{",
        r"control\s+egress\s+{"
    ]

    # Check if any of the patterns match
    for pattern in patterns:
        if re.search(pattern, contents):
            return True

    return False


def filter_p416(srcds, dstds):
    with open(srcds, "rt") as fin:
        lines = fin.readlines()

    result = []
    for line in lines:
        js = json.loads(line)
        contents = js["content"]
        if not is_p414(contents):
            result.append(line)

    with open(dstds, "wt") as fout:
        fout.writelines(result)



if __name__ == "__main__":
    filter_p416(sys.argv[1], sys.argv[2])
