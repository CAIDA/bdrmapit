#! /usr/bin/env python3
# written by Bradley Huffaker (2016.05.14)
import sys, getopt
import csv
import re
import bz2
import sqlite3

def Help():
    print (sys.argv[0],"[-ht]","sqllite.db")
    print ("      -c cvs-file")
    print ("   converts between the bdrmapIT database file and the ITDK format")
    print ("   -c assumes a comma seperated file (now deprecated) ")
    print ("   by default it assumes a sqllite file")

ip2nid = {}

def Main(argv):
    print ("# exec:"," ".join(argv))
    try:
        opts, args = getopt.getopt(argv, "htc:")
    except getopt.GetoptError:
        Help()
        sys.exit(-1)

    csv_fname = None
    nodes_fname = None
    bdrmapit_fname = None

    for opt, arg in opts:
        if opt == '-h':
            Help()
            sys.exit()
        if opt == "-c":
            cvs_fname = arg
        if opt == "-t":
            PrintTables(args)
            sys.exit();

    if len(args) > 0:
        bdrmapit_fname = args[0]

    if csv_fname is not None:
        Convert_Csv(cvs_fname)
    elif len(args) == 1:
        Convert_Sql(args[0])
    else:
        Help();
        sys.exit(-1)

    sys.exit(0)
############################################################
# whole thing to disk :(
############################################################

def PrintTables(args):
    if len(args) != 1:
        sys.stderr.write(sys.argv[0]+" [-ht] sqllite.db\n")
        sys.exit(-1);
    fname = args[0]
    sys.stderr.write("loading "+fname+"\n")
    con = sqlite3.connect(fname)
    cur = con.cursor()

    tables = []
    #cur.execute("SELECT name FROM my_db.sqlite_master WHERE type='table';")
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    for table in cur.fetchall():
        tables.append(table[0])

    for table in tables:
        print (table)
        cur.execute("SELECT * FROM "+table+";")
        col_name_list = [tuple[0] for tuple in cur.description]
        print ("   ",col_name_list)

def Convert_Sql(fname):
    sys.stderr.write("loading "+fname+"\n")
    con = sqlite3.connect(fname)
    cur = con.cursor()

    cur.execute("SELECT DISTINCT router, asn FROM annotation");
    for nid_asn in cur.fetchall():
        print ("node.AS",nid_asn[0], nid_asn[1])

def Convert_Csv(fname):
    with open(fname,"r") as filecvs:
        reader = csv.DictReader(filecvs)
        nid_current = 0
        asn_current = 0
        linenum = 1;
        for row in reader:
            nid = row['Router']
            asn = row['ASN']
            if nid_current != nid:
                print("node.AS", nid, asn)
            elif asn != asn_current:
                sys.stderr.write(fname+"["+str(linenum)+"] node N"+nid+" multiple ASN values "+asn+", "+asn_current+"\n")
            nid_current = nid
            asn_current = asn
            linenum += 1

Main(sys.argv[1:])
