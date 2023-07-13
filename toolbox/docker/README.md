# Docker maintenance scripts

## Docke image prune

Dangling docker images are common with our dockerized ETLs, because the top-most image layers contain ETL code. when that code changes, the previous layer is discarded, resulting in a dangling docker images that can use signficant disk space. This utility removes those dangling images.

```bash
$ image_prune.sh
```
