#!/bin/bash

if [ "$#" -ne 2 ]; then
		printf "Illegal number of parameters.\n\n"
		printf "Usage:\n	load_database.sh <graph> <file_manifest>\n"
		exit 1
fi

graph=$1
file_manifest=$2

for f in $(cat $file_manifest); do
		if [ ! -f $f ]; then
				echo "file $f does not exist!"
				exit 1
		fi
done

# grip drop --host test-grip-mongo:18202 $graph
# grip create --host test-grip-mongo:18202 $graph

gofast="--numInsertionWorkers 24 --writeConcern 0 --bypassDocumentValidation --host=test-mongo"

for f in $(cat $file_manifest | grep "Vertex"); do
		if [[ $f =~ \.gz$ ]]; then
				gunzip -c $f | mongoimport -d grip -c ${graph}_vertices --type json $gofast
		else
				mongoimport -d grip -c ${graph}_vertices --type json --file $f $gofast
		fi
done

for f in $(cat $file_manifest | grep "Edge"); do
		if [[ $f =~ \.gz$ ]]; then
				gunzip -c $f | mongoimport -d grip -c ${graph}_edges --type json $gofast
		else
				mongoimport -d grip -c ${graph}_edges --type json --file $f $gofast
		fi
done
