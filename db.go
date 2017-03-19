package main

import (
	"errors"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"time"

	"github.com/boltdb/bolt"
)

var dbs = struct {
	sync.RWMutex
	data map[string]*DBData
}{data: make(map[string]*DBData)}

type DBData struct {
	lastEdited time.Time
	db         *bolt.DB
}

func init() {
	go closeDBs()
}

func getDB(dbname string) (*bolt.DB, error) {
	dbs.Lock()
	defer dbs.Unlock()
	var err error
	if _, ok := dbs.data[dbname]; !ok {
		log.Debug("Opening %s", dbname)
		tempDB, err2 := bolt.Open(path.Join(dbpath, dbname+".db"), 0755, nil)
		dbs.data[dbname] = new(DBData)
		dbs.data[dbname].db = tempDB
		err = err2
	}
	dbs.data[dbname].lastEdited = time.Now()
	db := dbs.data[dbname].db
	return db, err
}

func closeDBs() {
	for {
		time.Sleep(10 * time.Second)
		dbs.Lock()
		toDelete := []string{}
		for dbname := range dbs.data {
			if time.Since(dbs.data[dbname].lastEdited).Seconds() > 10 {
				toDelete = append(toDelete, dbname)
			}
		}

		for _, dbname := range toDelete {
			log.Debug("Closing %s", dbname)
			dbs.data[dbname].db.Close()
			delete(dbs.data, dbname)
		}
		dbs.Unlock()
	}
}

func getNumberKeysInBucket(dbname string, bucket string) (n int, err error) {
	n = 0
	db, err := getDB(dbname)
	if err != nil {
		return n, err
	}

	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return errors.New("Bucket does not exist")
		}
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			n++
		}
		return nil
	})
	log.Trace("Found %d keys in bucket '%s' in db '%s'", n, bucket, dbname)
	return n, err
}

func getBucketNames(dbname string) (bucketNames []string, err error) {
	db, err := getDB(dbname)
	if err != nil {
		return bucketNames, err
	}

	err = db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
			bucketNames = append(bucketNames, string(name))
			return nil
		})
	})
	return bucketNames, err
}

func createDatabase(dbname string, buckets []string) error {
	db, err := getDB(dbname)
	if err != nil {
		return err
	}

	return db.Update(func(tx *bolt.Tx) error {
		for _, bucket := range buckets {
			_, err2 := tx.CreateBucketIfNotExists([]byte(bucket))
			if err2 != nil {
				return err2
			}
		}
		return nil
	})
}

// updateDatabase
func updateDatabase(dbname string, bucket string, keystore map[string]string) error {
	db, err := getDB(dbname)
	if err != nil {
		return err
	}

	return db.Update(func(tx *bolt.Tx) error {
		b, err2 := tx.CreateBucketIfNotExists([]byte(bucket))
		if err2 != nil {
			return err2
		}
		for key, value := range keystore {
			err2 := b.Put([]byte(key), compressStringToByte(value))
			if err2 != nil {
				return err
			}
		}
		return err
	})
}

func getKeysFromDatabase(dbname string, bucket string) (keys []string, err error) {
	db, err := getDB(dbname)
	if err != nil {
		return []string{}, err
	}

	numKeys := 0
	err = db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return errors.New("Bucket does not exist")
		}
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			numKeys++
		}

		keys = make([]string, numKeys)
		numKeys = 0
		c = b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			keys[numKeys] = string(k)
			numKeys++
		}
		return nil
	})
	return
}

func getFromDatabase(dbname string, bucket string, keys []string) (map[string]string, error) {
	keystore := make(map[string]string)

	db, err := getDB(dbname)
	if err != nil {
		return keystore, err
	}

	if len(keys) == 0 {
		// Get all keys
		err = db.View(func(tx *bolt.Tx) error {
			// Assume bucket exists and has keys
			b := tx.Bucket([]byte(bucket))
			if b == nil {
				return errors.New("Bucket does not exist")
			}
			c := b.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				if v != nil {
					keystore[string(k)] = decompressByteToString(v)
				}
			}
			return nil
		})
	} else {
		// Get specified keys
		err = db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(bucket))
			if b == nil {
				return errors.New("Bucket does not exist")
			}
			for _, key := range keys {
				v := b.Get([]byte(key))
				if v != nil {
					keystore[key] = decompressByteToString(v)
				}
			}
			return nil
		})
	}
	return keystore, err
}

func deleteDatabase(dbname string) error {
	// Check if there is a specific match in the dbpath, otherwise it may
	// be an attack
	files, _ := ioutil.ReadDir(path.Join(dbpath))
	foundDB := false
	for _, f := range files {
		if f.Name() == dbname+".db" {
			foundDB = true
		}
	}
	if !foundDB {
		return errors.New("Could not find '" + dbname + "'")
	}

	db, err := getDB(dbname)
	if err != nil {
		return err
	}
	db.Close()

	if _, err := os.Stat(path.Join(dbpath, dbname+".db")); os.IsNotExist(err) {
		return err
	}
	return os.Remove(path.Join(dbpath, dbname+".db"))
}

func deleteKeys(dbname string, bucket string, keys []string) error {
	db, err := getDB(dbname)
	if err != nil {
		return err
	}

	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return errors.New("Bucket does not exist")
		}
		for _, key := range keys {
			b.Delete([]byte(key))
		}
		return err
	})
}

func deleteBucket(dbname string, bucket string) error {
	db, err := getDB(dbname)
	if err != nil {
		return err
	}

	return db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket([]byte(bucket))
	})
}

func pop(dbname string, bucket string, n int) (map[string]string, error) {
	keystore := make(map[string]string)

	db, err := getDB(dbname)
	if err != nil {
		return keystore, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return errors.New("Bucket does not exist")
		}

		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			b.Delete(k)
			keystore[string(k)] = decompressByteToString(v)
			if len(keystore) == n {
				break
			}
		}
		return nil
	})
	return keystore, err
}

func moveBuckets(dbname string, bucket1 string, bucket2 string, keys []string) error {
	db, err := getDB(dbname)
	if err != nil {
		return err
	}

	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket1))
		if b == nil {
			return errors.New("Bucket does not exist")
		}
		b2, _ := tx.CreateBucketIfNotExists([]byte(bucket2))
		for _, key := range keys {
			val := b.Get([]byte(key))
			if val != nil {
				b.Delete([]byte(key))
				b2.Put([]byte(key), val)
			} else {
				return errors.New("Could not find key: " + key)
			}
		}
		return nil
	})
}

func hasKeys(dbname string, buckets []string, keys []string) (doesHaveKeyMap map[string]bool, err error) {
	doesHaveKeyMap = make(map[string]bool)

	db, err := getDB(dbname)
	if err != nil {
		return doesHaveKeyMap, err
	}

	for _, key := range keys {
		doesHaveKeyMap[key] = false
	}

	err = db.View(func(tx *bolt.Tx) error {
		for _, bucket := range buckets {
			b := tx.Bucket([]byte(bucket))
			if b == nil {
				continue
			}
			for _, key := range keys {
				v := b.Get([]byte(key))
				if v != nil {
					doesHaveKeyMap[string(key)] = true
				}
			}
		}
		return nil
	})
	return doesHaveKeyMap, err
}

func hasKey(dbname string, bucket string, key string) (doesHaveKey bool, err error) {
	doesHaveKey = false

	db, err := getDB(dbname)
	if err != nil {
		return doesHaveKey, err
	}

	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return errors.New("Bucket does not exist")
		}
		v := b.Get([]byte(key))
		if v != nil {
			doesHaveKey = true
		}
		return nil
	})
	return doesHaveKey, err
}
