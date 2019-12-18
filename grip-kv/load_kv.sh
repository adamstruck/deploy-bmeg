#!/bin/sh

set -e

if [ "$#" -ne 2 ]; then
		printf "Illegal number of parameters.\n\n"
		printf "Usage:\n	load_kv.sh <graph-name> <data-dir>\n"
		exit 1
fi

GRAPH=$1
DATAPATH=$2

cd $DATAPATH

if [ ! -d "outputs" ]; then
		echo "provided path does not contain 'outputs' directory"
		exit 1
fi

if [ ! -f "bmeg_file_manifest.txt" ]; then
		echo "provided path is missing 'bmeg_file_manifest.txt'"
		exit 1
fi

cat bmeg_file_manifest.txt | grep Vertex | xargs -n 1 -I {} grip load $GRAPH --vertex {}
cat bmeg_file_manifest.txt | grep Edge | xargs -n 1 -I {} grip load $GRAPH --edge {}
