// Package connect provides functionality for directly accessing a BoltDB server
// instance with simple functions for posting data, getting data (such as keys and buckets),
// moving data, deleting data, and popping data. To use, make sure that you have a boltdb-server up and running which you can do simply
// with
// ```
// go get github.com/schollz/boltdb-server
// $GOPATH/bin/boltdb-server
// ```
package connect

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

// Connection is the BoltDB server instance
type Connection struct {
	DBName  string
	Address string
}

// Open will load a connection to BoltDB
func Open(address, dbname string) (*Connection, error) {
	c := new(Connection)
	c.Address = address
	c.DBName = dbname
	resp, err := http.Get(c.Address + "/v1/uptime")
	if err != nil {
		return c, err
	}
	defer resp.Body.Close()
	return c, nil
}

// Post keys and values to database
func (c *Connection) Post(bucket string, keystore map[string]string) error {
	payloadBytes, err := json.Marshal(keystore)
	if err != nil {
		return err
	}
	body := bytes.NewReader(payloadBytes)

	req, err := http.NewRequest("POST", c.Address+"/v1/db/"+c.DBName+"/bucket/"+bucket+"/update", body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

// Get keys and values from database
func (c *Connection) Get(bucket string, keys []string) (map[string]string, error) {
	payloadBytes, err := json.Marshal(keys)
	if err != nil {
		return make(map[string]string), err
	}
	body := bytes.NewReader(payloadBytes)

	req, err := http.NewRequest("GET", c.Address+"/v1/db/"+c.DBName+"/bucket/"+bucket+"/some", body)
	if err != nil {
		return make(map[string]string), err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return make(map[string]string), err
	}
	defer resp.Body.Close()

	var target map[string]string
	err = json.NewDecoder(resp.Body).Decode(&target)
	if err != nil {
		return make(map[string]string), err
	}
	return target, nil
}

// GetAll keys and values from database
func (c *Connection) GetAll(bucket string) (map[string]string, error) {
	resp, err := http.Get(c.Address + "/v1/db/" + c.DBName + "/bucket/" + bucket + "/all")
	if err != nil {
		return make(map[string]string), err
	}
	defer resp.Body.Close()

	var target map[string]string
	err = json.NewDecoder(resp.Body).Decode(&target)
	if err != nil {
		return make(map[string]string), err
	}
	return target, nil
}

// GetKeys returns all keys from database
func (c *Connection) GetKeys(bucket string) ([]string, error) {
	resp, err := http.Get(c.Address + "/v1/db/" + c.DBName + "/bucket/" + bucket + "/keys")
	if err != nil {
		return []string{}, err
	}
	defer resp.Body.Close()

	var target []string
	err = json.NewDecoder(resp.Body).Decode(&target)
	return target, err
}

// Pop returns and deletes the first n keys from a bucket
func (c *Connection) Pop(bucket string, n int) (keystore map[string]string, err error) {
	resp, err := http.Get(fmt.Sprintf("%s/v1/db/%s/bucket/%s/pop?n=%d", c.Address, c.DBName, bucket, n))
	if err != nil {
		return keystore, err
	}
	defer resp.Body.Close()

	err = json.NewDecoder(resp.Body).Decode(&keystore)
	return keystore, err
}

// Pop returns and deletes the first n keys from a bucket
func (c *Connection) Move(bucket string, bucket2 string, keys []string) (err error) {
	type QueryJSON struct {
		FromBucket string   `json:"from_bucket"`
		ToBucket   string   `json:"to_bucket"`
		Keys       []string `json:"keys"`
	}
	moveJSON := new(QueryJSON)
	moveJSON.FromBucket = bucket
	moveJSON.ToBucket = bucket2
	moveJSON.Keys = keys

	payloadBytes, err := json.Marshal(moveJSON)
	if err != nil {
		return err
	}
	body := bytes.NewReader(payloadBytes)

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/v1/db/%s/move", c.Address, c.DBName), body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}
