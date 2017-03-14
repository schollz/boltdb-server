package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

// Testing
// curl -H "Content-Type: application/json" -X POST -d '{"bucket":"food","keystore":{"username":"xyz","password":"xyz"}}' 'http://zack:123@localhost:8080/'

// Payload is how the data is entered and returned from the BoltDB server
// DB is the name of the database file
// Bucket is the name of the bucket in the database
// Keystore is a map of the keys and values
type Payload struct {
	DB       string            `json:"db" binding:"required"`
	Bucket   string            `json:"bucket" binding:"required"`
	Keystore map[string]string `json:"keystore" binding:"required"`
}

func updateDatabase(db string, bucket string, keystore map[string]string) (string, bool) {
	for key, value := range keystore {
		log.Printf("Adding key '%s' into '%s'", key, bucket)
		fmt.Println(value)
	}
	return fmt.Sprintf("Updated %d keys in %s", len(keystore), bucket), true
}

func handleRequests(c *gin.Context) {
	fmt.Println(c.Request.Method)
	username, password, _ := c.Request.BasicAuth()
	if username != SpecifiedUsername || password != SpecifiedPassword {
		c.JSON(http.StatusForbidden, gin.H{
			"success": false,
			"message": "Incorrent credentials",
		})
		return
	}
	fmt.Println(username, password)
	var json Payload
	if c.BindJSON(&json) == nil {
		message := "Incorrect method"
		success := false
		if c.Request.Method == "POST" {
			message, success = updateDatabase(json.DB, json.Bucket, json.Keystore)
		}
		c.JSON(http.StatusOK, gin.H{
			"success": success,
			"message": message,
		})
	} else {
		c.JSON(http.StatusNotAcceptable, gin.H{
			"success": false,
			"message": "Cannot bind JSON",
		})
	}
}

var Port, SpecifiedUsername, SpecifiedPassword string

func main() {
	flag.StringVar(&SpecifiedUsername, "user", "", "port to use for server")
	flag.StringVar(&SpecifiedPassword, "pass", "", "port to use for server")
	flag.StringVar(&Port, "port", "8080", "port to use for server")
	flag.Parse()
	r := gin.Default()
	r.GET("/", handleRequests)    // Get keys from BoltDB
	r.POST("/", handleRequests)   // Post keys to BoltDB
	r.DELETE("/", handleRequests) // Delete keys in BoltDB

	fmt.Println("Listening on 0.0.0.0:8080")
	r.Run(":" + Port) // listen and serve on 0.0.0.0:8080
}
