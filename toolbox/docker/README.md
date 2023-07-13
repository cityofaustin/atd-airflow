# Docker maintenance scripts

## Docke image prune

Dangling docker images are common with our dockerized ETLs, because the top-most image layers contain ETL code. When that code changes, the previous layer is discarded, resulting in dangling docker images that can consume signficant disk space. This utility removes those dangling images.

```bash
$ image_prune.sh
```
