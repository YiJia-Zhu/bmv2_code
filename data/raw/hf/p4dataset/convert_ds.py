#!/usr/bin/env python3
import json
import sys

def main():
    with open(sys.argv[1], 'r') as fin:
        data = json.load(fin)


    lines = []
    for entry in data:
        line = json.dumps(entry)
        lines.append(line)

    with open(sys.argv[2], 'w') as fout:
        fout.write("\n".join(lines))


if __name__ == "__main__":
    main()
