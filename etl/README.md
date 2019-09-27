
# etl

* Verify that the graph files were mounted

```
$ dc exec etl bash -c "ls -lR  /etl/bmeg-data"
```

* Exec into the etl container, run the script

```
$ dc exec etl bash
# cd /etl
# load_database.sh <graph-name> ./bmeg-data/<release-dir>/bmeg_file_manifest.txt
```

* Run integration tests

```
cd /etl/bmeg-etl
BMEG_URL=grip:8201 BMEG_GRAPH=<graph-name> BMEG_CREDENTIAL_FILE=  pytest tests/integration
```
