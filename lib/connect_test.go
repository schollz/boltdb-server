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
	err = conn.Post("persons2", data)
	err = conn.Post("persons", data)
	if err != nil {
		t.Errorf(err.Error())
	}
	if _, err := os.Stat(path.Join("..", "dbs", "testdb.db")); os.IsNotExist(err) {
		t.Errorf("Problem creating directory")
	}

	err = conn.Move("persons2", "people", []string{"person1", "person2"})
	if err != nil {
		t.Errorf(err.Error())
	}
	data2, err := conn.GetAll("people")
	if err != nil {
		t.Errorf(err.Error())
	}
	if val, ok := data2["person2"]; ok {
		if val != "bob" {
			t.Errorf("Could not get bob back")
		}
	} else {
		t.Errorf("Problem with GetAll: %v", data2)
	}

	data2, err = conn.GetAll("persons")
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

	// Check if its there
	data3, err := conn.Get("persons", []string{"person1"})
	if len(data3) != 1 {
		t.Errorf("Should only recieve one thing")
	}
	err = conn.Delete("persons", []string{"person1"})
	if err != nil {
		t.Errorf(err.Error())
	}
	// Check if its there
	data4, err := conn.Get("persons", []string{"person1"})
	if err != nil {
		t.Errorf(err.Error())
	}
	if len(data4) != 0 {
		t.Errorf("Should be deleted")
	}

	// Check if its there
	err = conn.Post("people2", data)
	if err != nil {
		t.Errorf(err.Error())
	}
	data4, err = conn.MoveTopN("people2", "people3", 100)
	if err != nil {
		t.Errorf(err.Error())
	}
	if len(data4) != 2 {
		t.Errorf("Problem recieving the moved things")
	}
	data4, err = conn.GetAll("people2")
	if len(data4) != 0 {
		t.Errorf("Things didn't get moved from old bucket")
	}
	data4, err = conn.GetAll("people3")
	if len(data4) != 2 {
		t.Errorf("Things didn't get moved to new bucket")
	}

	err = conn.DeleteDatabase()
	if err != nil {
		t.Errorf(err.Error())
	}
	if _, err := os.Stat(path.Join("..", "dbs", "testdb.db")); err == nil {
		t.Errorf("Problem deleting DB")
	}
}
