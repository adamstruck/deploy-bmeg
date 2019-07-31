
# etl

## to drop and create a graph

Exec into the grip server directly:

```
dc exec grip grip drop <graph-name>
dc exec grip grip create <graph-name>
```

## to load the graph

* Verify that bmeg-etl has been cloned and mounted into the container

From `docker-compose.yml`

```
- /mnt/data2/bmeg/bmeg-etl:/bmeg-etl
```

* Verify that modifications were made to enable

From `bmeg-etl/scripts/load_database.sh`

```
# grip drop $graph
# grip create $graph

gofast="--numInsertionWorkers 8 --writeConcern 0 --bypassDocumentValidation --host=mongo"

```

* Verify that bmeg-etl/outputs was populated

```
$ dc exec etl bash -c "ls -lR  /bmeg-etl/outputs  | wc -l"
1675
```

* Exec into the etl container, run the script

```
$ dc exec etl bash
# cd /bmeg-etl
# scripts/load_database.sh <graph-name> scripts/bmeg_file_manifest.txt
```

* Run integration tests

```
# BMEG_URL=grip:8201 BMEG_GRAPH=bmeg_rc1_3 BMEG_CREDENTIAL_FILE=  python -m pytest tests/integration/
<graph-name>
```
