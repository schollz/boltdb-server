# boltdb-server
Server for a BoltDB file

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



