<p align="center">
<img 
    src="logo.png" 
    width="240" height="78" border="0" alt="BoltDB Server">
<br>
<a href="https://travis-ci.org/schollz/boltdb-server"><img src="https://img.shields.io/travis/schollz/boltdb-server.svg?style=flat-square" alt="Build Status"></a>
<a href="http://gocover.io/github.com/schollz/boltdb-server/connect"><img src="https://img.shields.io/badge/coverage-67%25-yellow.svg?style=flat-square" alt="Code Coverage"></a>
<a href="https://godoc.org/github.com/schollz/boltdb-server/connect"><img src="https://img.shields.io/badge/api-reference-blue.svg?style=flat-square" alt="GoDoc"></a>
</p>

<p align="center">a simple server for BoltDB databases</a></p>

BoltDB is a great utility for pure-Go keystore databases. This is a server and connection utility for it.

Getting Started
===============

## Installing

To start using the server, install Go and run `go get`:

```sh
$ go get -u github.com/schollz/boltdb-server/...
```

This will retrieve the library and the server.

## Run

Run the server using

```sh
$GOPATH/bin/boltdb-server
```

Then you can use the server directly (see API below) or plug in a Go program using the connect package (see tests).

## API
        
### `GET /v1/db/<dbname>/bucket/<bucket>/all`

Returns all the keys and values for the specified bucket and database.
Response:

```json
{
  "key1":"value1",
  "key2":"value2",
  "key3":"value3"
}
```

### `GET /v1/db/<dbname>/bucket/<bucket>/some?keys=key1,key2`

Returns all keys and values for the specified bucket and database and specified keys
Response:

```json
{
  "key1":"value1",
  "key2":"value2"
}
```

### `GET /v1/db/<dbname>/bucket/<bucket>/pop?n=2`

Returns first *n* keys and values for the specified bucket and database, and **deletes them from the bucket**.
Response:

```json
{
  "key1":"value1",
  "key2":"value2"
}
```
### `GET /v1/db/<dbname>/bucket/<bucket>/keys`

Returns a list of all keys in the specified bucket and database.

Response:

```json
{
  "keys":["key1","key2","key3"]
}
```



## Performance