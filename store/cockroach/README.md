This is an implementation of cockroach storage engine support.(Package dependency is not considered currently.)

#### run tidb with cockroach

* run `make` to build tidb server with cockroach support
* start a cockroach cluster(see [cockroach](https://github.com/cockroachdb/cockroach#running-a-multi-node-cluster) for more details.)
* run tidb_server

```shell
tidb_server -store="cockroach" -path="127.0.0.1:26257?certs=./certs"
```

** Note: ** 

`certs` must be specified because we only support secure mode of cockroach now and `certs` must be the same as which cockroach start with. 

* connect tidb-server using mysql client

```shell
mysql -h 127.0.0.1 -P 4000 -u root -D test
```