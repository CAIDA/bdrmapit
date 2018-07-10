# bdrmapIT
This is the code to run <tt>bdrmapIT</tt>. There are a lot of files but only a few need to be run from the command line.

## Setting up the Environment
It is recommended to run <tt>bdrmapIT</tt> in its own environment, such as [Virtualenv](https://virtualenv.pypa.io/en/stable/) of [Anaconda](https://www.anaconda.com/):

### Virtualenv
```bash
$ virtualenv bdrmapit  # Create the environment
$ cd bdrmapit  # Enter the environment subdirectory
$ source bin/activate  # Activate the environment
$ pip install -r  ../requirememts.txt  # Install required packages
```

### Anaconda
```bash
$ conda create -n bdrmapit python=3  # Create the environment
$ conda activate bdrmapit  # Activate the environment
$ pip install -r requirements.txt  # Install required packages
```

## Generating IP-to-AS Mappings
The first step is to generate the IP-to-AS mappings by combining BGP announcements, RIR extended delegation files, and IXP prefixes. The simplest way, and the way I've been doing it, is to use the [ip2as](ip2as.md) file. The usage instructions are in that file.

## Process Traceroute Files
In order to generate the graph used by bdrmapIT, the the [parser](parser.md) script processes the orginal traceroute files. This creates a single sqlite database file with four tables:

## address
|Column|Type|Description|
|---|---|---|
|addr|TEXT|Interface IP address exposed by traceroute|
|num|INT|The numerical representation of addr|

Every address exposed by one of the traceroutes is recorded in this table. The numerical representation is currently unused but might be used in a later version to determine address adjacencies.

## adjacency
|Column|Type|Description|
|---|---|---|
|hop1|TEXT|The first hop in the pair|
|hop2|TEXT|The second hop in the pair|
|distance|INT|The distance between the hops. Distance is either 1 to indicate immediately adjacent, or 2 to indicate that there is one or more hop-gaps between them.|
|type|INT|The ICMP Type of hop2|
|direction|INT|Currently always the same, but might be used in a later version|

This table records all hop pairs. Hop pairs are any two hops with no hops, private address hops, or unresponsive hops between them. We ignore any hop pairs following the first occurance of a cycled address (a repeated address separated where the two instances of the address are separated by at least one other address).

## Running bdrmapIT
The last step is to actually run the [bdrmapit.py](bdrmapit.md) script, which runs the bdrmapIT algorithm. This produces a single (possibly large) CSV output file, described in its readme.
