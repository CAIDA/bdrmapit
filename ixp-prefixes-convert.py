#!  /usr/bin/env python
import os, re, sys

def main():
    if len(sys.argv) < 2:
        print sys.argv[0]," ixp_prefix.merged.txt > ixp_prefixes.txt"
        sys.exit(-1)
    process(sys.argv[1])


def process(filename):
    print filename
    re_prefix = re.compile("^(\d+\.\d+\.\d+\.\d+\/\d+)")
    with open(filename) as fin:
        for line in fin:
            match = re_prefix.search(line)
            if match:
                print match.group(1)

main()
