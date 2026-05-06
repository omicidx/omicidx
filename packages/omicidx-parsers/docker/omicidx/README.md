# running

## Local docker usage

```sh
docker run -ti -e GCLOUD_PROJECT=GCP_PROJECT_NAME \
	   -v $HOME/.config:/root/.config DOCKER_HASH
```

# Inside container

```sh
omicidx-cli --help
```
