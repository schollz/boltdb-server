<p align="center">
<img
src="logo.png"
width="260" height="80" border="0" alt="BoltDB Server">
<br>
<a href="https://travis-ci.org/schollz/boltdb-server"><img src="https://img.shields.io/travis/schollz/boltdb-server.svg?style=flat-square" alt="Build Status"></a>
<a href="http://gocover.io/github.com/schollz/boltdb-server/connect"><img src="https://img.shields.io/badge/coverage-72%25-green.svg?style=flat-square" alt="Code Coverage"></a>
<a href="https://godoc.org/github.com/schollz/boltdb-server/connect"><img src="https://img.shields.io/badge/api-reference-blue.svg?style=flat-square" alt="GoDoc"></a>
</p>

<p align="center">A fancy server for Bolt databases</a></p>

*boltdb-server* is a server and package (`connect`) for interfacing
with [boltdb/bolt](https://github.com/boltdb/bolt), a pure-Go embedded key/value database. 

Features
========

- Automatic compression of values
- Simple API for getting, setting, moving, popping and deleting BoltDB data
- Package for adding to your Go programs

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

Then you can use the server directly (see API below) or plug in a Go program using the connect package, [see tests for more info](https://github.com/schollz/boltdb-server/blob/master/connect/connect_test.go).

## API

```
// Get map of buckets and the number of keys in each
GET /v1/db/<db>/stats

// Get list of all buckets 
GET /v1/db/<db>/buckets

// Get all keys and values from a bucket
GET /v1/db/<db>/bucket/<bucket>/numkeys

// Get all keys and values from a bucket
GET /v1/db/<db>/bucket/<bucket>/all

// Get all keys and values specified by ?keys=key1,key2 or by JSON
GET /v1/db/<db>/bucket/<bucket>/some

// Delete and return first n keys
GET /v1/db/<db>/bucket/<bucket>/pop?n=X

// Get all keys in a bucket
GET /v1/db/<db>/bucket/<bucket>/keys", handleGetKeys) 

// Return boolean of whether it has key
GET /v1/db/<db>/bucket/<bucket>/haskey/<key>

// Return boolean of whether any buckets contain any keys specified by JSON
GET /v1/db/<db>/haskeys

// Delete database file
DELETE /v1/db/<db>

// Delete bucket
DELETE /v1/db/<db>/bucket/<bucket>

// Delete keys, where keys are specified by JSON []string
DELETE /v1/db/<db>/bucket/<bucket>/keys

// Updates a database with keystore specified by JSON
POST /v1/db/<db>/bucket/<bucket>/update

// Move keys, with buckets and keys specified by JSON
POST /v1/db/<db>/move

// Create buckets specified by JSON
POST /v1/db/<db>/create
```