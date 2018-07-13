# parser.py
This script processes raw **warts** and **atlas** traceroute files in parallel, extracting addresses, interface-level links (adjacencies), interface-destination AS pairs (destpairs), and whether interfaces normally appear adjacent or not (distances).
I have been using this script on a machine with 160 cores, 500 GB or memory, and SSD storage.
I typically use a pool of 30 processes, and each one runs at around 100% CPU utilization.
The script runs fast on that machine, but it might be significantly slower with other configurations.

## Options
|Field|Required|Description|Default|
|---|---|---|---|
|-i, --ip2as|Yes|The output file produced by the [ip2as.py](ip2as.md) script.|None|
|-W, --warts|No|Filename of a file containing Warts traceroute filenames, one per line.|None|
|-A, --atlas|No|Filename of a file containing Atlas traceroute filenames, one per line.|None|
|-o, --output-dir|No|Directory where output files will be written. The intermediate files will also be written here.|Current directory|
|-k, --keep|No|Indicates that the intermediate files should not be deleted.|False|
|-p, --poolsize|No|Number of processes to use when processing the traceroute files. Each process will work on a file at a time.|1|
|--no-combine|No|Do not combine the individual file outputs. Useful when processing files for separate <tt>bdrmapIT</tt> runs. Overrides the <tt>-k</tt> options, forcing it to True.|False|

## Notes
* When specifying filenames with the -W or -A option, they should either be absolute or relative to the current directory.
* The traceroute files can be uncompressed, compressed with gzip (having a filename ending in .gz), or compressed with bzip (having a filename ending in .bz2).
* Each process in the pool processes a traceroute file at a time.
The intermediate results from that file are written to the hard drive.
Once all of the intermediate files are aggregated, the final output is written.
* When processing thousands of traceroute files (such as for an ITDK), the memory used can be significant.
I imagine this will be quite slow if the aggregated results do not fit in memory.
* The intermediate results are written to, and read from, the hard drive.
I've been using quick SSDs, and I'm not sure how well it will perform with HDDs.
* If a single file is being processed, it will be processed and written by the main process without spawning any child processes.

## Output
The output is a single [<tt>sqlite</tt>](https://www.sqlite.org/index.html) database file with four tables:

### address
|Column|Type|Description|
|---|---|---|
|<tt>addr</tt>|TEXT|Interface IP address exposed by traceroute|
|<tt>num</tt>|INT|The numerical representation of <tt>addr</tt>|

Every address exposed by one of the traceroutes is recorded in this table. The numerical representation is currently unused but might be used in a later version to determine address adjacencies.

### adjacency
|Column|Type|Description|
|---|---|---|
|<tt>hop1</tt>|TEXT|The first hop in the pair|
|<tt>hop2</tt>|TEXT|The second hop in the pair|
|<tt>distance</tt>|INT|The distance between the hops. Distance is either 1 to indicate immediately adjacent, or 2 to indicate that there is one or more hop-gaps between them.|
|<tt>type</tt>|INT|The ICMP Type of <tt>hop2</tt>|
|<tt>direction</tt>|INT|Currently always the same, but might be used or removed in a later version|

This table records all hop pairs. Hop pairs are any two hops with no hops, private address hops, or unresponsive hops between them. We ignore any hop pairs following the first occurance of a cycled address (a repeated address separated where the two instances of the address are separated by at least one other address).

### destpair
|Column|Type|Description|
|---|---|---|
|<tt>addr</tt>|TEXT|Interface IP address exposed by traceroute|
|<tt>asn</tt>|INT|IP-to-AS mapping for traceroute destination IP address|
|<tt>echo</tt>|BOOLEAN|True if the ICMP Type of <tt>addr</tt>'s response was ECHO|
|<tt>exclude</tt>|BOOLEAN|Currently unused, but might be used or removed in a later version|

For each traceroute, we store every IP address seen along with the destination AS of the traceroute. The destination AS is simply the IP-to-AS mapping of the traceroute destination.

### distance
|Column|Type|Description|
|---|---|---|
|<tt>hop1</tt>|TEXT|The first hop in the pair|
|<tt>hop2</tt>|TEXT|The second hop in the pair|
|<tt>distance</tt>|INT|Greater than 0 indicates the hops are typically adjacent. Less than 0 indicates they are typically separated by at least 1 other hop.|

This table is used to track two potentially adjacent hops across all of the traceroutes. This prevents hops that are typically multiple hops away from being inferred as immediately aadjacent hops.