# Retrieve BGP
This is a script for downloading the BGP RIBs files necessary for the IP-to-AS mapping.
Theses files can be used as input for the [ip2as.py](ip2as.md) script.
Currently, it used the midnight full table RIBs file for each of the BGP VPs.

## Options
|Option|Required|Description|Default|
|---|---|---|---|
|-p, --pool|No|Maximum number of concurrent downloads|1|
|-s, --start|Yes|Start date for the downloads. Most conventional date representations are accepted.|N/A|
|-e, --end|No|End date for the downloads.|Start Date|
|-d, --dir|No|Directory in which to save the downloaded files.|Current directory|
|-n, --nc|No|Use the `wget` -nc (--no-clobber) option which preserves files that were already downloaded.

## Notes
This script requires `wget` for dowloading the files.
It is of course possible to implement the downloads in python, but it would've taken longer, and I only use UNIX-based OSes.