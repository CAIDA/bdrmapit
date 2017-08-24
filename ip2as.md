# ip2as.py
The [ip2as.py](ip2as.py) script generates the IP-to-AS mappings used by the other bdrmapIT scripts.

## Options
|Option|Required|Description|
|---|---|---|
|-p, --prefixes|Yes|Unix-style regex for files containing IP prefixes and the AS mapping associated with that prefix. They must be in the same format as the CAIDA [prefix-to-AS](http://data.caida.org/datasets/routing/README.txt) files.|
|-r, --rir|No|Unix-style regex for RIR extended delegation files. These should be the files provided by the RIRs. The RIR files are used to supplement the prefixes.|
|-i, --ixp-prefixes|No|File with IXP IP prefixes, one per line.|
|-a, --as2org|No|A file containing AS-to-Organization mappings, in the same format as the CAIDA [as2org](http://data.caida.org/datasets/as-organizations/README.txt) files. These files are used to help resolve MOAS prefixes.|
|-R, --rels|No|AS relationships file in the format of the CAIDA [AS relationships](http://data.caida.org/datasets/as-relationships/README.txt) files. Not required unless the cone option is supplied.|
|-c, --cone|No|Customer cone file in the format of the CAIDA [customer cone](http://data.caida.org/datasets/as-relationships/README.txt) files. Not required unless the rels option is supplied.|
|-o, --output|No|Output file for the IP-to-AS mappings. The default is stdout.|

## Output
It produces a CSV with the following fields:

|Field|Description|
|---|---|
|Prefix|An IP prefix in standard notation <network>/prefixlen. Ex: x.x.x.0/24|
|ASN|AS number associated with that prefix.|

If a file is specified using the -o option, the CSV will be written to that file, otherwise it will be written to stdout.

## Notes
When a prefix is associated with multiple origin ASes, one AS is selected.
If the AS2Org file is provided, and all of the ASes are part of the same organization, and a single AS appears most frequently with that prefix, then it is chosen. Otherwise, or when no AS2Org file is supplied, it uses the rels and cone files to try to determine if a single AS is a customer of all the others. If so, it selects that AS. Finally, the AS most frequently associated with the prefix is chosen in the event the other methonds failed, or if no files were provided.