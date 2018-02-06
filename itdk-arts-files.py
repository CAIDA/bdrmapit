#!  /usr/bin/env python
__author__ = "Bradley Huffaker"
__email__ = "<bhuffake@caida.org>"
# This software is Copyright 〓 2017 The Regents of the University of
# California. All Rights Reserved. Permission to copy, modify, and
# distribute this software and its documentation for educational, research
# and non-profit purposes, without fee, and without a written agreement is
# hereby granted, provided that the above copyright notice, this paragraph
# and the following three paragraphs appear in all copies. Permission to
# make commercial use of this software may be obtained by contacting:
#
# Office of Innovation and Commercialization
#
# 9500 Gilman Drive, Mail Code 0910
#
# University of California
#
# La Jolla, CA 92093-0910
#
# (858) 534-5815
#
# invent@ucsd.edu
#
# This software program and documentation are copyrighted by The Regents of
# the University of California. The software program and documentation are
# supplied “as is”, without any accompanying services from The Regents. The
# Regents does not warrant that the operation of the program will be
# uninterrupted or error-free. The end-user understands that the program
# was developed for research purposes and is advised not to rely
# exclusively on the program for any reason.
#
# IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
# DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES,
# INCLUDING LOST PR OFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
# DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF
# THE POSSIBILITY OF SUCH DAMAGE. THE UNIVERSITY OF CALIFORNIA SPECIFICALLY
# DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE
# SOFTWARE PROVIDED HEREUNDER IS ON AN “AS IS” BASIS, AND THE UNIVERSITY OF
# CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES,
# ENHANCEMENTS, OR MODIFICATIONS.
import os, re, sys
import bz2

def main():
    if len(sys.argv) < 2:
        print (sys.argv[0]," itdk-nodes-file ")
        print ("    used to pull out the warts files from")
        sys.exit(-1)
    process(sys.argv[1])


def process(filename):

    re_prefix = re.compile("#\s+-P\s+([^\s]+warts.gz)")
    re_comment = re.compile("^#");

    if re.search("bz2$", filename): 
        with open(filename,"rb") as fin:
            de = bz2.BZ2Decompressor()
            line = ""
            for data in iter(lambda : fin.read(100 * 1024), b''):
                decompressed = de.decompress(data)
                if decompressed:
                    for char in list(decompressed.decode('ascii')):
                        line = line + char
                        if char is '\n':
                            process_line(line, re_prefix, re_comment)
                            line = ""
            process_line(line, re_prefix, re_comment)
    else:
        with open(filename) as fin:
            for line in fin:
                process_line(line, re_prefix, re_comment)

def process_line(line, re_prefix, re_comment):
    match = re_prefix.search(line)
    if match:
        print (match.group(1))
    if not re_comment.search(line):
        sys.exit()

main()
