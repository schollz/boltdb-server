package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// Testing
// curl -H "Content-Type: application/json" -X POST -d '{"bucket":"food","keystore":{"username":"xyz","password":"xyz"}}' 'http://zack:123@localhost:8080/'

var port string
var gzipOn bool

func main() {
	flag.BoolVar(&gzipOn, "gzip", true, "use compression")
	flag.StringVar(&port, "port", "8080", "port to use for server")
	flag.Parse()

	startTime := time.Now()

	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()
	r.GET("/v1/api", func(c *gin.Context) {
		c.String(200, `

/v1/db/:dbname/stats // Get map of buckets and the number of keys in each
/v1/db/:dbname/buckets // Get list of all buckets
/v1/db/:dbname/bucket/:bucket/numkeys  // Get all keys and values from a bucket (no parameters)

`)
	})
	r.GET("/v1/uptime", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"uptime": time.Since(startTime).String(),
		})
	})
	r.GET("/v1/db/:dbname/stats", handleGetDBStats)                  // Get map of buckets and the number of keys in each
	r.GET("/v1/db/:dbname/buckets", handleGetBuckets)                // Get list of all buckets
	r.GET("/v1/db/:dbname/bucket/:bucket/numkeys", handleGetNumKeys) // Get all keys and values from a bucket (no parameters)
	r.GET("/v1/db/:dbname/bucket/:bucket/all", handleGet)            // Get all keys and values from a bucket (no parameters)
	r.GET("/v1/db/:dbname/bucket/:bucket/some", handleGet)           // Get all keys and values specified by ?keys=key1,key2 or by JSON
	r.GET("/v1/db/:dbname/bucket/:bucket/pop", handlePop)            // Delete and return first n keys + values, where n specified by ?n=100
	r.GET("/v1/db/:dbname/bucket/:bucket/keys", handleGetKeys)       // Get all keys in a bucket (no parameters)
	r.GET("/v1/db/:dbname/bucket/:bucket/haskey/:key", handleHasKey) // Return boolean of whether it has key
	// r.GET("/v1/db/:dbname/bucket/:bucket/data", getDataArchive)   // Creates archive with keys as filenames and values as contents, returns archive
	//
	r.DELETE("/v1/db/:dbname", handleDeleteDatabase)                 // Delete database file (no parameters)
	r.DELETE("/v1/db/:dbname/bucket/:bucket", handleDeleteBucket)    // Delete bucket (no parameters)
	r.DELETE("/v1/db/:dbname/bucket/:bucket/keys", handleDeleteKeys) // Delete keys, where keys are specified by JSON []string
	//
	r.POST("/v1/db/:dbname/bucket/:bucket/update", handleUpdate) // Updates a database with keystore specified by JSON
	r.POST("/v1/db/:dbname/move", handleMove)                    // Move keys, with buckets and keys specified by JSON

	log.Printf("Listening on 0.0.0.0:%s\n", port)
	r.Run(":" + port) // listen and serve on 0.0.0.0:8080
}

func handleHasKey(c *gin.Context) {
	dbname := c.Param("dbname")
	bucket := c.Param("bucket")
	key := c.Param("key")
	doesHaveKey, err := hasKey(dbname, bucket, key)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, doesHaveKey)
}

func handleGetDBStats(c *gin.Context) {
	dbname := c.Param("dbname")
	bucketNames, err := getBucketNames(dbname)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	stats := make(map[string]int)
	for _, bucket := range bucketNames {
		stats[bucket], err = getNumberKeysInBucket(dbname, bucket)
		if err != nil {
			c.String(http.StatusInternalServerError, err.Error())
			return
		}
	}
	c.JSON(http.StatusOK, stats)
}

func handleGetNumKeys(c *gin.Context) {
	dbname := c.Param("dbname")
	bucket := c.Param("bucket")
	n, err := getNumberKeysInBucket(dbname, bucket)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, n)
}

func handleGetBuckets(c *gin.Context) {
	dbname := c.Param("dbname")
	bucketNames, err := getBucketNames(dbname)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, bucketNames)
}

func handleDeleteDatabase(c *gin.Context) {
	dbname := c.Param("dbname")
	err := deleteDatabase(dbname)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.String(http.StatusOK, "Deleted database")
}

func handleDeleteBucket(c *gin.Context) {
	dbname := c.Param("dbname")
	bucket := c.Param("bucket")
	err := deleteBucket(dbname, bucket)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.String(http.StatusOK, "Deleted bucket")
}

func handleDeleteKeys(c *gin.Context) {
	dbname := c.Param("dbname")
	bucket := c.Param("bucket")
	var keys []string
	if c.BindJSON(&keys) != nil {
		c.String(http.StatusBadRequest, "Problem binding keys")
		return
	}
	err := deleteKeys(dbname, bucket, keys)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.String(http.StatusOK, "Deleted keys")
}

func handleUpdate(c *gin.Context) {
	dbname := c.Param("dbname")
	bucket := c.Param("bucket")
	var json map[string]string
	if c.BindJSON(&json) != nil {
		c.String(http.StatusBadRequest, "Problem binding keystore")
		return
	}
	err := updateDatabase(dbname, bucket, json)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.String(http.StatusOK, fmt.Sprintf("Inserted %d things into %s", len(json), bucket))
}

func handleGetKeys(c *gin.Context) {
	dbname := c.Param("dbname")
	bucket := c.Param("bucket")
	keystore, err := getKeysFromDatabase(dbname, bucket)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, keystore)
}

func handlePop(c *gin.Context) {
	dbname := c.Param("dbname")
	bucket := c.Param("bucket")
	nQuery := c.DefaultQuery("n", "0")
	num, err := strconv.Atoi(nQuery)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	if num <= 0 {
		c.String(http.StatusBadRequest, "Must specify n > 0")
		return
	}
	keystore, err := pop(dbname, bucket, num)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, keystore)
}

func handleGet(c *gin.Context) {
	dbname := c.Param("dbname")
	bucket := c.Param("bucket")
	keysQuery := c.DefaultQuery("keys", "")
	json := []string{}
	if c.BindJSON(&json) != nil && keysQuery != "" {
		json = strings.Split(keysQuery, ",")
	}
	// If requested some without providing keys, throw error
	if len(json) == 0 && strings.Contains(c.Request.RequestURI, "some") {
		c.String(http.StatusBadRequest, "Must provide keys")
		return
	}
	// Get keys and values
	keystore, err := getFromDatabase(dbname, bucket, json)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, keystore)
	return
}

func handleMove(c *gin.Context) {
	dbname := c.Param("dbname")
	type QueryJSON struct {
		FromBucket string   `json:"from_bucket"`
		ToBucket   string   `json:"to_bucket"`
		Keys       []string `json:"keys"`
	}
	var json QueryJSON
	if c.BindJSON(&json) != nil {
		c.String(http.StatusBadRequest, "Must provide keys, from_bucket and to_bucket")
		return
	}
	// Get keys and values
	err := moveBuckets(dbname, json.FromBucket, json.ToBucket, json.Keys)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, fmt.Sprintf("Moved keys"))
}
