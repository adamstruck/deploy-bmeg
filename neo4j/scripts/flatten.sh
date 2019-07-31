find /mnt/data2/bmeg/bmeg-etl/outputs -name "*.Vertex.json.gz" | xargs -n 1 -P 24 -I {} sh -c 'zcat {} | /home/strucka/bin/jq -c '"'"'[ leaf_paths as $path | {"key": $path | join("."), "value": getpath($path)} ] | from_entries'"'"' | gzip - > $(echo {} | sed -E "s~(/mnt/data2/bmeg/bmeg-etl/outputs/)(.*)~/mnt/data2/bmeg/testing/neo4j/outputs-flattened/\2~g")'
# find /mnt/data2/bmeg/bmeg-etl/outputs -name "*.Vertex.json.gz" | head | xargs -n 1 -P 24 -I {} bash -c 'zcat {} | head | gzip - > $(echo {} | sed -E "s~(/mnt/data2/bmeg/bmeg-etl/outputs/)(.*)~/mnt/data2/bmeg/testing/neo4j/outputs-flattened/\2~g")'

# jq '[ (leaf_paths | map( select(. | type == "string"))) as $path | {"key": $path | join("."), "value": getpath($path)}] | from_entries'
# jq '[ leaf_paths as $path | {"key": $path | join("."), "value": getpath($path)} ] | from_entries'
