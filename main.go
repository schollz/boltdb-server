package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

// Testing
// curl -H "Content-Type: application/json" -X POST -d '{"bucket":"food","keystore":{"username":"xyz","password":"xyz"}}' 'http://zack:123@localhost:8080/'

// Payload is how the data is entered and returned from the BoltDB server
type Payload struct {
	Bucket   string            `json:"bucket" binding:"required"`
	Keystore map[string]string `json:"keystore" binding:"required"`
}

func main() {
	r := gin.Default()
	r.GET("/v1/bucket/:bucket/key/:key", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "pong",
		})
	})
	r.POST("/", func(c *gin.Context) {
		username, password, _ := c.Request.BasicAuth()
		fmt.Println(username, password)
		var json Payload
		if c.BindJSON(&json) == nil {
			for key, value := range json.Keystore {
				log.Printf("Adding key '%s' into '%s' for '%s'", key, json.Bucket, username)
				fmt.Println(value)
			}
			c.JSON(http.StatusOK, gin.H{
				"success": true,
				"message": fmt.Sprintf("Updated %d keys in %s", len(json.Keystore), json.Bucket),
			})
		} else {
			c.JSON(http.StatusNotAcceptable, gin.H{
				"success": false,
				"message": "Cannot bind JSON",
			})
		}
	})
	fmt.Println("Listening on 0.0.0.0:8080")
	r.Run() // listen and serve on 0.0.0.0:8080
}
