# Load BMEG Into a Graph Database

### Supported Databases
  - [GRIP](https://bmeg.github.io/grip/)
  - [Dgraph](https://dgraph.io/)
  - [Neo4j]((https://neo4j.com/))


### Load data into GRIP


### Load data into Dgraph

```
# start the dgraph zero server
docker-compose up -d zero

# generate commands to transform data to RDF
python3 dgraph/to_rdf.py cmd-gen --manifest ./bmeg_file_manifest.txt --cmd-outdir ./dgraph --rdf-outdir ./dgraph/outputs-rdf

# run transform commands
bash dgraph/load_db.txt

# load the data 
dgraph bulk --schema ./dgraph/outputs-rdf/schema.rdf --rdfs ./dgraph/outputs-rdf/data.rdf --zero zero:5080 --out ./tmp_dgraph
mv ./tmp_dgraph/0/p ./data/dgraph/alpha

# start the dgraph alpha server and dgraph UI
docker-compose up -d alpha
docker-compose up -d ratel

# load the schema
dgraph live --schema ./dgraph/outputs-rdf/schema.rdf --zero zero:5080 --dgraph alpha:9080
```


### Load data into Neo4j

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
