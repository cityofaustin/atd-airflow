# Docker maintenance scripts

## Docke image prune

Removes dangling docker images, which accumulate as various ETLs' docker images are overwritten with new code.

```bash
$ image_prune.sh
```
