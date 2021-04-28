# mergify-engine production

[![mergify-engine syncer cron](https://github.com/Mergifyio/mergify-engine-prod/actions/workflows/syncer.yml/badge.svg)](https://github.com/Mergifyio/mergify-engine-prod/actions/workflows/syncer.yml)

This is the repository deployed in Heroku to run Mergify engine in production.

The upstream code is deployed using a git submodule in the `mergify-engine` directory.
The upstream repository is automatically updated using [a GitHub Action named syncer](https://github.com/Mergifyio/mergify-engine-prod/actions/workflows/syncer.yml).

We use a `git-submodule` buildpack to install the deps of the submodule:

```
$ heroku buildpacks:clear
$ heroku buildpacks:set https://github.com/Mergifyio/heroku-buildpack-git-submodule.git
$ heroku buildpacks:set https://github.com/DataDog/heroku-buildpack-datadog.git
$ heroku buildpacks:set heroku/python

```

When this repository is updated, Heroku automatically triggers a new deployment using the latest commit of the `main` branch.

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
