# bdrmapIT
This is the code to run bdrmapIT. There are a lot of files but only a few need to be run from the command line.

To run bordermap it, you can use virtualenv
```bash
$ virtualenv run
$ cd run
```

This will install the libraries you need
```bash
$ source bin/activate
$ pip install -r  ../requirememts.txt
```

## Generating IP-to-AS Mappings
The first step is to generate the IP-to-AS mappings by combining BGP announcements, RIR extended delegation files, and IXP addresses. The simplest way, and the way I've been doing it, is to use the [ip2as](ip2as.md) file. The usage instructions are in that file.

## numpy on FREEBSD
In order to build numpy on FREEBSD
```bash
$ tar xzf /usr/ports/distfiles/numpy-numpy-v1.13.3_GH0.tar.gz 
$ cd numpy-1.13.3/
$ python setup.py build --fcompiler=gnu95
$ python setup.py install
$ pip install lxml requests ipyparallel networkx pandas py-radix
```

I also had to rebuild lang/python36 math/cblas math/blas math/openblas
math/lapack by source using gcc6  ( something you'll never need to do, but
it's good to have the information down.

## Process Traceroute Files
In order to generate the graph used by bdrmapIT, the the [parser](parser.md) script processes the orginal traceroute files. This creates 4 files:

|File|Description|
|---|---|
|addrs.txt|The unique addresses seen in the traceroutes, one per line.|
|adjs.csv|The interface adjacencies (IP links) between hops.|
|dps.csv|The unique (interface, destination AS) pairs.|
|dists.csv|Indicates whether interface adjacencies are actually neighbors, or seperated by other routers.|

## Running bdrmapIT
The last step is to actually run the [bdrmapit.py](bdrmapit.md) script, which runs the bdrmapIT algorithm. This produces a single (possibly large) CSV output file, described in its readme.

## Converting to ITDK format
The output file can be converted to the ITDK format with itdk-format.py.
```bash
python utils/itdk-format.py bordermapit-output > itdk-formated.txt
```
