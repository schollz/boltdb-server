package connect

import (
	"bytes"
	"encoding/json"
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
	if err != nil {
		return []string{}, err
	}
	return target, nil
}
