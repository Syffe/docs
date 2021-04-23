# mergify-engine production

[![mergify-engine syncer cron](https://github.com/Mergifyio/mergify-engine-prod/actions/workflows/syncer.yml/badge.svg)](https://github.com/Mergifyio/mergify-engine-prod/actions/workflows/syncer.yml)

Engine is located in mergify-engine as git submodule

We use a git-submodule and subdir buildpack to install deps of the submodule

```
$ heroku buildpacks:clear
$ heroku buildpacks:set https://github.com/Mergifyio/heroku-buildpack-git-submodule.git
$ heroku buildpacks:set https://github.com/Mergifyio/heroku-buildpack-subdir.git

```

All others buildpacks must go in .buildpacks

## Running the submodule syncer workflow on GitHub runners via CLI:

```
$ gh workflow run "mergify-engine syncer cron"
```

## Running the submodule syncer workflow locally with docker:

With https://github.com/nektos/act

```
$ act -j syncer -s GITHUB_TOKEN=${GITHUB_TOKEN}
```
