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
In order to generate the graph used by <tt>bdrmapIT</tt>, the the [parser](parser.md) script processes the orginal traceroute files. Description of arguments, options, and output is described there.

## Running bdrmapIT
The last step is to actually run the [bdrmapit.py](bdrmapit.md) script, which runs the bdrmapIT algorithm. This produces a single [<tt>sqlite</tt>](https://www.sqlite.org/index.html) database file described in its documentation.
