package connect

import (
	"fmt"
	"os"
	"path"
	"testing"
)

// Start server with
// go build; .\boltdb-server.exe

func TestGeneral(t *testing.T) {
	conn, err := Open("http://localhost:8080", "testdb")
	if err != nil {
		t.Errorf(err.Error())
	}

	data := make(map[string]string)
	data["zack"] = "canada"
	data["jessie"] = "usa"
	err = conn.Post("people_locations", data)
	if err != nil {
		t.Errorf(err.Error())
	}
	if _, err := os.Stat(path.Join("..", "dbs", "testdb.db")); os.IsNotExist(err) {
		t.Errorf("Problem creating directory")
	}

	data2, err := conn.GetAll("people_locations")
	if err != nil {
		t.Errorf(err.Error())
	}
	if val, ok := data2["zack"]; ok {
		if val != "canada" {
			t.Errorf("Could not get zack back")
		}
	} else {
		t.Errorf("Problem with GetAll: %v", data2)
	}

	keys, err := conn.GetKeys("people_locations")
	if err != nil {
		t.Errorf(err.Error())
	}
	if len(keys) != 2 {
		fmt.Println(keys)
		t.Errorf("Problem getting the two keys back")
	}

}
