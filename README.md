# cbtool

Couchbase tool.

## Installation

```
$ composer global require mehr-als-nix/cbtool
```

You can simple create a phar package by using `kherge/box`:
```
$ box build
```

## Configuration

see `./config.ini`
```
cb.bucket=default
cb.username=Administrator
cb.password=Password
 
sql.database=database
sql.username=root
sql.password=
 
import.tables=
import.createViews=true
import.typefield=
import.fieldcase=
```

## Commands

### flush

Flushes a bucket defined in `config.ini`:
```
$ cbtool flush config.ini
```

### import

Imports doctrine based database tables defined in `config.ini` to couchbase:
```
$ cbtool import config.ini
```


### info

Shows info of a bucket defined in `config.ini`:
```
$ cbtool info config.ini
```
