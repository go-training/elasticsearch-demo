# elasticsearch-demo

elasticsearch api with golang

## Starting a single node cluster with Dockeredit

See the docs for [detail information](https://www.elastic.co/guide/en/elasticsearch/reference/current/docker.html#docker-cli-run-dev-mode)

```sh
docker run -p 9200:9200 -p 9300:9300 \
  -e "discovery.type=single-node" \
  docker.elastic.co/elasticsearch/elasticsearch:7.5.2
```

run the demo code:

```sh
go run main.go
```
