#!/bin/sh

cd /data
cat bmeg_file_manifest.txt | grep Vertex > vertex_manifest.txt
cat bmeg_file_manifest.txt | grep Edge > edge_manifest.txt
{ time grip kvload bmeg_rc2 --db ./db --edge-manifest edge_manifest.txt --vertex-manifest vertex_manifest.txt ; } 2> timings/kvload_time.txt
grip server --config /config/grip_config.yml
tail -f /dev/null
