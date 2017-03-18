package connect

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"testing"
)

// Start server with
// go build; .\boltdb-server.exe
var testingServer = "http://localhost:8080"

func BenchmarkPostOne(b *testing.B) {
	os.Remove(path.Join("..", "dbs", "testbench.db"))
	conn, _ := Open(testingServer, "testbench")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m := map[string]string{"key" + strconv.Itoa(i): "value" + strconv.Itoa(i)}
		conn.Post("benchkeys", m)
	}
}

func BenchmarkGetAll1(b *testing.B) {
	conn, _ := Open(testingServer, "testbench")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn.GetAll("benchkeys")
	}
}

func BenchmarkPost1000(b *testing.B) {
	conn, _ := Open(testingServer, "testbench")
	m := make(map[string]string)
	for i := 0; i < 1000; i++ {
		m["key"+strconv.Itoa(i)] = "value" + strconv.Itoa(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn.Post("benchkeys", m)
	}
}

func BenchmarkGetAll1000(b *testing.B) {
	conn, _ := Open(testingServer, "testbench")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn.GetAll("benchkeys")
	}
}

func BenchmarkPost100000(b *testing.B) {
	conn, _ := Open(testingServer, "testbench")
	m := make(map[string]string)
	for i := 0; i < 100000; i++ {
		m["key"+strconv.Itoa(i)] = "value" + strconv.Itoa(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn.Post("benchkeys", m)
	}
}

func BenchmarkGetAll100000(b *testing.B) {
	conn, _ := Open(testingServer, "testbench")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn.GetAll("benchkeys")
	}
}

func BenchmarkGetKeys(b *testing.B) {
	conn, _ := Open(testingServer, "testbench")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn.GetKeys("benchkeys")
	}
}

func BenchmarkGetTwo(b *testing.B) {
	conn, _ := Open(testingServer, "testbench")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn.Get("benchkeys", []string{"key1", "key20"})
	}
}

func BenchmarkHasKey(b *testing.B) {
	conn, _ := Open(testingServer, "testbench")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn.HasKey("benchkeys", "key1")
	}
}

func TestGeneral(t *testing.T) {
	// Test opening DB that doesnt exist
	conn, err := Open("http://asdkfjalsjdflkasjdf", "testdb")
	if err == nil {
		t.Errorf("Should not be able to connect")
	}

	// Test opening DB
	conn, err = Open(testingServer, "testdb")
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

	// Test HasKeys
	data["bob"] = "brazil"
	data["jill"] = "antarctica"
	err = conn.Post("people_locations3", data)
	hasKeysMap, err := conn.HasKeys([]string{"people_locations", "people_locations3"}, []string{"zack", "jessie", "bob", "jim"})
	if err != nil {
		t.Errorf(err.Error())
	}
	if hasKeysMap["jim"] != false || hasKeysMap["bob"] != true || len(hasKeysMap) != 4 {
		t.Errorf("Problem checking whether buckets have keys")
	}

	// Test HasKey
	hasKey, err := conn.HasKey("people_locations", "zack")
	if err != nil {
		t.Errorf(err.Error())
	}
	if hasKey == false {
		t.Errorf("Incorrectly checking whether key exists")
	}
	hasKey, err = conn.HasKey("people_locations", "askjdflasjdlkfj")
	if err != nil {
		t.Errorf(err.Error())
	}
	if hasKey != false {
		t.Errorf("Incorrectly checking whether key exists")
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

	// Test Pop
	keystore, err := conn.Pop("people_locations", 1)
	if err != nil {
		t.Errorf(err.Error())
	}
	if val, ok := keystore["jessie"]; ok {
		if val != "usa" {
			t.Errorf("Could not get jessie back")
		}
	} else {
		t.Errorf("Problem with Pop: %v", keystore)
	}
	keys, err = conn.GetKeys("people_locations")
	if err != nil {
		t.Errorf(err.Error())
	}
	if len(keys) != 1 {
		fmt.Println(keys)
		t.Errorf("Problem getting the one keys back")
	}

	// Test Move
	keys, _ = conn.GetKeys("people_locations")
	err = conn.Move("people_locations", "people_locations2", keys)
	if err != nil {
		t.Errorf(err.Error())
	}
	keys, err = conn.GetKeys("people_locations2")
	if err != nil {
		t.Errorf(err.Error())
	}
	if len(keys) != 1 {
		t.Errorf("Problem getting the one keys back")
	}
	keys, err = conn.GetKeys("people_locations")
	if err != nil {
		t.Errorf(err.Error())
	}
	if len(keys) != 0 {
		t.Errorf("Problem getting the one keys back")
	}

	// Test getting bucket that doesn't exist
	_, err = conn.GetKeys("asldkfjaslkdjf")
	if err == nil {
		t.Errorf("Should throw error, bucket does not exist")
	}

}
