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
There is a deditated process (one for each -abde option) that combines the intermediate results by reading the intermediate files, and then deleting them if the -k option is not used.
Once all of the intermediate files are aggregated, the final output is written.
This means that the total number of processes running at one time is the poolsize plus the number of -abde options invoked.
* When processing thousands of traceroute files (such as for an ITDK), the memory used can be significant.
I imagine this will be quite slow if the aggregated results do not fit in memory.
* The intermediate results are written to, and read from, the hard drive.
I've been using quick SSDs, and I'm not sure how well it will perform with HDDs.
* If a single file is being processed, it will be processed and written by the main process without spawning any child processes.