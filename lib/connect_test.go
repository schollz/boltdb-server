package connect

import (
	"os"
	"path"
	"testing"
)

// Start server with
// go build; .\boltdb-server.exe -user zack -pass 123
func TestGeneral(t *testing.T) {
	_, err := Open("http://localhost:8080", "testdb", "zack", "123jkjk")
	if err == nil {
		t.Errorf("Should throw error for wrong password")
	}
	conn, err := Open("http://localhost:8080", "testdb", "zack", "123")
	if err != nil {
		t.Errorf(err.Error())
	}

	data := make(map[string]string)
	data["person1"] = "zack"
	data["person2"] = "bob"
	err = conn.Post("persons", data)
	if err != nil {
		t.Errorf(err.Error())
	}
	if _, err := os.Stat(path.Join("..", "dbs", "testdb.db")); os.IsNotExist(err) {
		t.Errorf("Problem creating directory")
	}

	data2, err := conn.GetAll("persons")
	if err != nil {
		t.Errorf(err.Error())
	}
	if val, ok := data2["person2"]; ok {
		if val != "bob" {
			t.Errorf("Could not get bob back")
		}
	} else {
		t.Errorf("Problem with GetAll")
	}

	toDelete := make(map[string]string)
	toDelete["person1"] = ""
	// Check if its there
	data3, err := conn.Get("persons", toDelete)
	if len(data3) != 1 {
		t.Errorf("Should only recieve one thing")
	}
	err = conn.Delete("persons", toDelete)
	if err != nil {
		t.Errorf(err.Error())
	}
	// Check if its there
	data4, err := conn.Get("persons", toDelete)
	if len(data4) != 0 {
		t.Errorf("Should be deleted")
	}

	err = conn.DeleteDatabase()
	if err != nil {
		t.Errorf(err.Error())
	}
	if _, err := os.Stat(path.Join("..", "dbs", "testdb.db")); err == nil {
		t.Errorf("Problem deleting DB")
	}
}
