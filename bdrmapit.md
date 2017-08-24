# bdrmapit.py
The [bdrmapit.py](bdrmapit.py) runs the bdrmapIT algorithm.

## Options
|Option|Required|Default|Description|
|---|---|---|---|
|-a, --adj|Yes|None|The adjacency CSV file produced by parser.py|
|-d, --dp|Yes|None|The destpairs CSV file produced by parser.py|
|-e, --dist|Yes|None|The distances CSV file produced by parser.py|
|-n, --nodes|No|None|An inferred router (nodes) file in the standard CAIDA format (see below).|
|-i, --ip2as|Yes|None|The output file produced by the ip2as.py script.|
|-A, --as2org|No|None|A file containing AS-to-Organization mappings, in the same format as the CAIDA [as2org](http://data.caida.org/datasets/as-organizations/README.txt) files. These mappings are used to identify siblings.|
|-R, --rels|No|None|AS relationships file in the format of the CAIDA [AS relationships](http://data.caida.org/datasets/as-relationships/README.txt) files.|
|-c, --cone|No|None|Customer cone file in the format of the CAIDA [customer cone](http://data.caida.org/datasets/as-relationships/README.txt) files.|
|-o, --output|No|stdout|Filename where the output CSV will be written.|


## Output
It produces a CSV with the following fields:

|Field|Description|
|---|---|
|Router|Inferred router identifier. Either the identifier in the specified nodes file, otherwise an identifier created by the script of the form M<*number*>.|
|Interface|The IP interface address.|
|ASN|The node AS annotation.|
|Org|The AS-to-Org mapping of ASN.|
|ConnASN|The interface AS annotation.|
|ConnOrg|The AS-to-Org mapping of ConnASN.|
|RUpdate|Indicates how ASN was derived:  -1) Based only on the node's interfaces,  0) The last hop heuristic, or  1) Using subsequent interfaces.|
|IUpdate|Indicates how ConnASN was derived:  -2) The origin AS is different from ASN,  -1) Based only on the interface's origin AS, or  1) Using preceding nodes.|

If a file is specified using the -o option, the CSV will be written to that file, otherwise it will be written to stdout.

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