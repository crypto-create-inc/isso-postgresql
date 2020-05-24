Isso – a commenting server similar to Disqus
============================================

Postgresql support added to original Isso.

## Installation

Please refer to the origianl Isso document.

## Postgresql Setting
In `isso/defaults.ini`, set:

```
db-type = sqlite
dbpath = postgres://user:password@localhost:5432/db (Replace with your postgresql path)
```