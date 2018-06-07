# Running <tt>bdrmapIT</tt>
The [bdrmapit.py](bdrmapit.py) runs the <tt>bdrmapIT</tt> algorithm. The arguments can either be specified on the command line or in a configuration file. If using a configuration file, more than one bdrmapit run can be specified. Additionally, they can optionally be run in parallel.

Parallel execution is useful when the graph is relatively small (< a few million interfaces). However, it is expected that everything will slow considerably if the graphs do not fit in memory.

## Configuration File
The configuration file can be either a CSV or Excel spreadsheet. It must have the following columns, along with a header row as the first row. Extra columns are fine and will be ignored.

|Column|Required|Description|
|---|---|---|
|addrs|Yes|The addresses file produced by parser.py|
|adjs|Yes|The adjacency CSV file produced by parser.py|
|dps|Yes|The destpairs CSV file produced by parser.py|
|dists|Yes|The distances CSV file produced by parser.py|
|nodes|No|An alias resolution file in the CAIDA ITDK nodes file format (see below). Can optionally be a blank field.|
|ip2as|Yes|The output file produced by the ip2as.py script.|
|as2org|No|A file containing AS-to-Organization mappings, in the same format as the CAIDA [as2org](http://data.caida.org/datasets/as-organizations/README.txt) files. These mappings are used to identify siblings.|
|rels|Yes|AS relationships file in the format of the CAIDA [AS relationships](http://data.caida.org/datasets/as-relationships/README.txt) files.|
|cone|Yes|Customer cone file in the format of the CAIDA [customer cone](http://data.caida.org/datasets/as-relationships/README.txt) files.|
|output|Yes|Filename where the output sqlite database will be written. The file will be overwritten without warning if it exists.|

## Options
The command line options are specified below. Options where the required field is marked <tt>C</tt> are only required when not using a configuration file.

|Option|Required|Default|Description|
|---|---|---|---|
|-b, --addrs|C| |See column <tt>addrs</tt> above.|
|-a, --adsj|C| |See column <tt>adjs</tt> above.|
|-d, --dps|C| |See column <tt>dps</tt> above.|
|-e, --dists|C| |See column <tt>dists</tt> above.|
|-n, --nodes|No| |See column <tt>nodes</tt> above.|
|-i, --ip2as|C| |See column <tt>ip2as</tt> above.|
|-A, --as2org|C| |See column <tt>as2org</tt> above.|
|-R, --rels|C| |See column <tt>rels</tt> above.|
|-c, --cone|C| |See column <tt>cone</tt> above.|
|-o, --output|C| |See column <tt>output</tt> above.|
|-i, --iterations|No|-1|Maximum iterations of the graph refinement loop. If less than 0, it will loop until a repeated state.|
|--config|No| |Configuration file in the format specified above. The columns in the file override anyting specified on the command line.|
|--processes|No|1|Number of parallel processes. Greater than 1 will result in parallel execution.|


## Output
It produces an sqlite database with a single table named annotation. Its columns are:

|Field|Type|Description|
|---|---|---|
|addr|text|IP address of the interface.|
|router|text|Router identifier. If there were no aliases specified for addr, this will be the same as the addr field.|
|asn|int|The router's AS annotation, i.e. which AS operates the router.|
|org|text|AS2Org mapping for asn.|
|conn_asn|int|The interface's annotation, i.e., which network operates the router(s) connected to addr.|
|conn_org|text|AS2Org mapping for conn_asn.|
|iasn|int|The origin AS for addr.|
|iorg|text|AS2Org mapping for iasn.|
|rtype|int|Used for testing and debugging. Will be removed without notice.|

An inter-AS link can be easily identified by querying for <tt>addrs</tt> with a different <tt>asn</tt> and <tt>conn_asn</tt>. As an example, the query:
```sqlite
SELECT * FROM annoation WHERE asn != conn_asn
```
will return all interfaces used for interdomain links.

### Annotations
The two primary goals of <tt>bdrmapIT</tt> are annotating each node (group of interfaces on the same router) with an AS, and annotating each individual interface with an AS.
The node annotation indicated the AS inferred to operate the node.
If the annotation is AS3356, then <tt>bdrmapIT</tt> thinks that Level3 operates that router.

Alternatively, the interface annotation indicates the AS inferred to operate the other side of the link.
For node, *N*, with interface, *i*, *N* might have the annotation AS3356, and *i* might have the annotation AS174.
In this situation, <tt>bdrmapIT</tt> inferred that *i* is an interface on a router operated by Level3, and used to connect to a router operated by Cogent.

When determining the interface annotation, we assume that if it differs from the node annotation it is used for a point-to-point link.
Therefore, if its origin AS is not the same as the node annotation, we annotate the interface with its origin AS.

## Notes
The nodes file format, as explained in the ITDK readme, is:
> The nodes file lists the set of interfaces that were inferred to be on each router.
>
> Format: node <node_id>:   <i1>   <i2>   ...   <in>
>
> Example: node N33382:  4.71.46.6 192.8.96.6 0.4.233.32 
>
> Each lines indicates that a node node_id has interfaces i_1 to i_n.
Interface addresses in 224.0.0.0/3 (IANA reserved space for multicast) are not real addresses.
They were artificially generated to identify potentially unique non-responding interfaces in traceroute paths.
>
> The IPv6 dataset uses IPv6 multicast addresses (FF00::/8) to indicate non-responding interfaces in traceroute paths.