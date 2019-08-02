# Load BMEG info [Neo4j](https://neo4j.com/)

# Load BMEG into [dgraph](https://dgraph.io/)

This guide assumes the directory structure:

```
.
├── bmeg_file_manifest.txt
├── docker-compose.yml
├── data
│   ├── neo4j
├── neo4j
│   ├── Dockerfile
│   ├── plugins
│   │   └── apoc-3.4.0.7-all.jar
│   ├── queries.md
│   ├── README.md
│   ├── to_csv.py
│   └── requirements.txt
```

These commands are assumed to be run from the parent directory.

```
# start the neo4j server
docker-compose up -d neo4j

# exec into the container
docker exec -it neo4j bash
cd /etl

# generate commands to transform data to RDF
python3 neo4j/to_csv.py cmd-gen --manifest ./bmeg_file_manifest.txt --cmd-outdir ./neo4j --csv-outdir ./neo4j/outputs-csv

# run transform commands and load the data into neo4j
# an import report will be genereated in the working directory. 
bash neo4j/load_db.txt
```
