# bdrmapIT
This is the code to run bdrmapIT. There are a lot of files but only a few need to be run from the command line.

Running <tt>bdrmapIT</tt> requires at least python3.6. (Developement and tested for 3.6+, but might work with older versions.)

## Using virtualenv
```bash
$ virtualenv run
$ cd run
```

This will install the libraries you need
```bash
$ source bin/activate
$ pip install -r ../requirememts.txt
```

### Using Anaconda
```bash
$ conda create -n bdrmapit python=3
$ conda activate bdrmapit
```

This will install the libraries you need
```bash
$ pip install -r requirememts.txt
```

## Compiling and Installing Cython extensions
Presuming you have a C compiler, this step should be straightforward. In the top-level directory, run the [setup.py](setup.py) script. The following command should create the extensions necessary to run all of the other scripts.
```bash
python setup.py build_ext --inplace
```

## Generating IP-to-AS Mappings
The first step is to generate the IP-to-AS mappings by combining BGP announcements, RIR extended delegation files, and IXP addresses. The simplest way, and the way I've been doing it, is to use the [ip2as](ip2as.md) file. The usage instructions are in that file.

## Process Traceroute Files
In order to generate the graph used by bdrmapIT, the the [parser](parser.md) script processes the orginal traceroute files. This creates 4 files:

|File|Description|
|---|---|
|addrs.txt|The unique addresses seen in the traceroutes, one per line.|
|adjs.csv|The interface adjacencies (IP links) between hops.|
|dps.csv|The unique (interface, destination AS) pairs.|
|dists.csv|Indicates whether interface adjacencies are actually neighbors, or seperated by other routers.|

## Running bdrmapIT
The last step is to actually run the [bdrmapit.py](bdrmapit.md) script, which runs the bdrmapIT algorithm. This produces a single (possibly large) sqlite database file, described in its readme.
