# mergify-engine production

[![mergify-engine syncer cron](https://github.com/Mergifyio/mergify-engine-prod/actions/workflows/syncer.yml/badge.svg)](https://github.com/Mergifyio/mergify-engine-prod/actions/workflows/syncer.yml)

This is the repository deployed in Heroku to run Mergify engine in production.

The upstream code is deployed using a git submodule in the `mergify-engine` directory.
The upstream repository is automatically updated using [a GitHub Action named syncer](https://github.com/Mergifyio/mergify-engine-prod/actions/workflows/syncer.yml).

When this repository is updated `.github/workflows/docker.yaml` workflow will:

* builds a docker images
* test their starts
* pushes them to Heroku docker registry
* triggers a Heroku container release with the new images

## Building docker image locally

```
$ ./docker-build-test.sh -t my-testing-image --target saas-web .
```

Targets list:
* saas-web
* saas-worker-shared
* saas-worker-dedicated
* onpremise

## Running the submodule syncer workflow on GitHub runners via CLI:

```
$ gh workflow run "mergify-engine syncer cron"
```

This will trigger the `syncer` GitHub Action that will update the submodule.

## Running the submodule syncer workflow locally with Docker:

Using https://github.com/nektos/act you can test the `syncer` action locally:

```
$ act -j syncer -s GITHUB_TOKEN=${GITHUB_TOKEN}
```
