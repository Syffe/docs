# mergify-engine

This repository contains Mergify engine code with the infrastructure to build
Docker images for customers and deploy it on Heroku in production.

## Building docker image locally

```
$ ./docker-build-test.sh -t my-testing-image --target saas-web .
```

Targets list:
* saas-web
* saas-worker-shared
* saas-worker-dedicated
* onpremise
