# mergify-engine production

[![mergify-engine syncer cron](https://github.com/Mergifyio/mergify-engine-prod/actions/workflows/syncer.yml/badge.svg)](https://github.com/Mergifyio/mergify-engine-prod/actions/workflows/syncer.yml)

Engine is located in mergify-engine as git submodule

We use a subdir buildpack to install deps of the submodule

```
$ heroku buildpacks:clear
$ heroku buildpacks:set https://github.com/negativetwelve/heroku-buildpack-subdir
```

All others buildpacks must go in .buildpacks

## Running it manually:

With https://github.com/nektos/act

```
$ act -j syncer -s GITHUB_TOKEN=${GITHUB_TOKEN}
```
