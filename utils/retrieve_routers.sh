#!/bin/bash

while getopts :m:y:h FLAG; do
  case $FLAG in
    m)  #set option "a"
      MONTH=$OPTARG
      ;;
    y)  #set option "b"
      YEAR=$OPTARG
      ;;
    h)  #show help
      echo "\t-m MONTH"
      echo "\t-y YEAR"
      ;;
    \?) #unrecognized option - show help
      echo -e \\n"Option -${BOLD}$OPTARG${NORM} not allowed."
      HELP
      ;;
  esac
done

newdir="caida/itdk/$YEAR/$(printf "%02d" $MONTH)"
mkdir -p $newdir
cd $newdir
wget --user amarder@seas.upenn.edu --password abjl6565 -r -l 1 -np -nH -nc --cut-dirs=2 -A "midar-iff.*.bz2" "https://topo-data.caida.org/ITDK/ITDK-$YEAR-$(printf "%02d" $MONTH)/"