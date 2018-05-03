#! /usr/bin/env python
# written by Bradley Huffaker (2016.05.14)
import sys, getopt
import csv

def Help():
    print sys.argv[0],"-d date [-O output_dir]"
    print "   converts between the bdrmapIT format and the ITDK format"

def Main(argv):
    ParseArguments(argv)

def ParseArguments(argv):
    try:
        opts, args = getopt.getopt(argv, "h")
    except getopt.GetoptError:
        Help()
        sys.exit(-1)

    if len(argv) < 1:
        Help();
        sys.exit(-1)

    for opt, arg in opts:
        if opt == '-h':
            Help()
            sys.exit()

    Convert(argv[0])

def Convert(filename):
    with open(filename,"r") as filecvs:
        reader = csv.DictReader(filecvs)
        nid_current = 0
        asn_current = 0
        linenum = 1;
        for row in reader:
            nid = row['Router']
            asn = row['ASN']
            if nid_current != nid:
                print "node.AS",nid,asn
            elif asn != asn_current:
                sys.stderr.write(filename+"["+str(linenum)+"] node N"+nid+" multiple ASN values "+asn+", "+asn_current+"\n")
            nid_current = nid
            asn_current = asn
            linenum += 1
    
Main(sys.argv[1:])


