package connect

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
)

// Payload is how the data is entered and returned from the BoltDB server
// DB is the name of the database file
// Bucket is the name of the bucket in the database
// Keystore is a map of the keys and values
type Payload struct {
	DB       string            `json:"db" binding:"required"`
	Bucket   string            `json:"bucket" binding:"required"`
	Keystore map[string]string `json:"keystore" binding:"required"`
}

type ServerResponse struct {
	Success  bool              `json:"success"`
	Message  string            `json:"message"`
	Keystore map[string]string `json:"keystore"`
}

// Connection is the BoltDB server instance
type Connection struct {
	DBName             string
	Address            string
	Username, Password string
}

// Open will load a jsonstore from a file.
func Open(address, dbname, username, password string) (*Connection, error) {
	c := new(Connection)
	c.Address = address
	c.DBName = dbname
	c.Username = username
	c.Password = password

	data := Payload{
		DB:       "",
		Bucket:   "",
		Keystore: make(map[string]string),
	}
	payloadBytes, err := json.Marshal(data)
	if err != nil {
		return c, err
	}
	body := bytes.NewReader(payloadBytes)

	req, err := http.NewRequest("PUT", c.Address+"/v1", body)
	if err != nil {
		return c, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(c.Username, c.Password)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return c, err
	}
	defer resp.Body.Close()

	var target ServerResponse
	err = json.NewDecoder(resp.Body).Decode(&target)
	if err != nil {
		return c, err
	}
	if !target.Success {
		return c, errors.New(target.Message)
	}

	return c, nil
}

// Post data to database
func (c *Connection) Post(bucket string, keystore map[string]string) error {
	data := Payload{
		DB:       c.DBName,
		Bucket:   bucket,
		Keystore: keystore,
	}
	payloadBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}
	body := bytes.NewReader(payloadBytes)

	req, err := http.NewRequest("POST", c.Address+"/v1", body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(c.Username, c.Password)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var target ServerResponse
	err = json.NewDecoder(resp.Body).Decode(&target)
	if err != nil {
		return err
	}
	if !target.Success {
		return errors.New(target.Message)
	}

	return nil
}

// Get data to database
func (c *Connection) Get(bucket string, keystore map[string]string) (map[string]string, error) {
	data := Payload{
		DB:       c.DBName,
		Bucket:   bucket,
		Keystore: keystore,
	}
	payloadBytes, err := json.Marshal(data)
	if err != nil {
		return keystore, err
	}
	body := bytes.NewReader(payloadBytes)

	req, err := http.NewRequest("GET", c.Address+"/v1", body)
	if err != nil {
		return keystore, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(c.Username, c.Password)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return keystore, err
	}
	defer resp.Body.Close()

	var target ServerResponse
	err = json.NewDecoder(resp.Body).Decode(&target)
	if err != nil {
		return keystore, err
	}
	if !target.Success {
		return keystore, errors.New(target.Message)
	}
	keystore = target.Keystore
	return keystore, nil
}

// GetAll data to database
func (c *Connection) GetAll(bucket string) (map[string]string, error) {
	keystore := make(map[string]string)
	data := Payload{
		DB:       c.DBName,
		Bucket:   bucket,
		Keystore: keystore,
	}
	payloadBytes, err := json.Marshal(data)
	if err != nil {
		return keystore, err
	}
	body := bytes.NewReader(payloadBytes)

	req, err := http.NewRequest("GET", c.Address+"/v1", body)
	if err != nil {
		return keystore, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(c.Username, c.Password)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return keystore, err
	}
	defer resp.Body.Close()

	var target ServerResponse
	err = json.NewDecoder(resp.Body).Decode(&target)
	if err != nil {
		return keystore, err
	}
	if !target.Success {
		return keystore, errors.New(target.Message)
	}
	keystore = target.Keystore
	return keystore, nil
}

// DeleteDatabase database
func (c *Connection) DeleteDatabase() error {
	data := Payload{
		DB:       c.DBName,
		Bucket:   "-special-delete-",
		Keystore: make(map[string]string),
	}
	payloadBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}
	body := bytes.NewReader(payloadBytes)
	req, err := http.NewRequest("DELETE", c.Address+"/v1", body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(c.Username, c.Password)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var target ServerResponse
	err = json.NewDecoder(resp.Body).Decode(&target)
	if err != nil {
		return err
	}
	if !target.Success {
		return errors.New(target.Message)
	}
	return nil
}

// Delete data to database
func (c *Connection) Delete(bucket string, keystore map[string]string) error {
	data := Payload{
		DB:       c.DBName,
		Bucket:   bucket,
		Keystore: keystore,
	}
	payloadBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}
	body := bytes.NewReader(payloadBytes)

	req, err := http.NewRequest("DELETE", c.Address+"/v1", body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(c.Username, c.Password)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var target ServerResponse
	err = json.NewDecoder(resp.Body).Decode(&target)
	if err != nil {
		return err
	}
	if !target.Success {
		return errors.New(target.Message)
	}
	return nil
}
