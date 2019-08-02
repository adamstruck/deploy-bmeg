# Load BMEG into [dgraph](https://dgraph.io/)

This guide assumes the directory structure:

```
.
├── bmeg_file_manifest.txt
├── docker-compose.yml
├── data
│   ├── dgraph
│   │   ├── alpha
│   │   └── zero
├── dgraph
│   ├── outputs-rdf
│   ├── README.md
│   ├── requirements.txt
│   └── to_rdf.py
```

These commands are assumed to be run from the parent directory.

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
