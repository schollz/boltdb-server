package main

import (
	"errors"
	"fmt"
	"os"
	"path"

	"github.com/boltdb/bolt"
)

// updateDatabase
func updateDatabase(dbname string, bucket string, keystore map[string]string) error {
	db, err := bolt.Open(path.Join("dbs", dbname+".db"), 0600, nil)
	if err != nil {
		return err
	}
	defer db.Close()

	return db.Update(func(tx *bolt.Tx) error {
		b, err2 := tx.CreateBucketIfNotExists([]byte(bucket))
		if err2 != nil {
			return err2
		}
		for key, value := range keystore {
			err2 := b.Put([]byte(key), []byte(value))
			if err2 != nil {
				return err
			}
		}
		return err
	})
}

func getKeysFromDatabase(dbname string, bucket string) ([]string, error) {
	db, err := bolt.Open(path.Join("dbs", dbname+".db"), 0600, nil)
	if err != nil {
		return []string{}, err
	}
	defer db.Close()

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
		return nil
	})
	if err != nil {
		return []string{}, err
	}

	keys := make([]string, numKeys)
	numKeys = 0
	err = db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return errors.New("Bucket does not exist")
		}
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			keys[numKeys] = string(k)
			numKeys++
		}
		return nil
	})
	if err != nil {
		return []string{}, err
	}

	return keys, err
}

func getFromDatabase(dbname string, bucket string, keys []string) (map[string]string, error) {
	keystore := make(map[string]string)

	db, err := bolt.Open(path.Join("dbs", dbname+".db"), 0600, nil)
	if err != nil {
		return keystore, err
	}
	defer db.Close()

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
				keystore[string(k)] = string(v)
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
					keystore[key] = string(v)
				}
			}
			return nil
		})
	}
	return keystore, err
}

func init() {
	os.Mkdir("dbs", 0644)
}

func deleteFromDatabase(dbname string, bucket string, keys map[string]string) error {
	db, err := bolt.Open(path.Join("dbs", dbname+".db"), 0600, nil)
	if err != nil {
		return err
	}
	defer db.Close()

	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return errors.New("Bucket does not exist")
		}
		for key := range keys {
			b.Delete([]byte(key))
		}
		return err
	})
}

func pop(dbname string, bucket string, n int) (map[string]string, error) {
	keystore := make(map[string]string)

	db, err := bolt.Open(path.Join("dbs", dbname+".db"), 0600, nil)
	if err != nil {
		return keystore, err
	}
	defer db.Close()

	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return errors.New("Bucket does not exist")
		}

		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			b.Delete(k)
			keystore[string(k)] = string(v)
			if len(keystore) == n {
				break
			}
		}
		return nil
	})
	return keystore, err
}

func moveKeys(dbname string, bucket1 string, bucket2 string, keys map[string]string) (string, bool) {
	db, err := bolt.Open(path.Join("dbs", dbname+".db"), 0600, nil)
	if err != nil {
		return fmt.Sprintf("Error: '%s'", err.Error()), false
	}
	defer db.Close()

	numMovedKeys := 0
	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket1))
		if b == nil {
			return errors.New("Bucket does not exist")
		}
		b2, _ := tx.CreateBucketIfNotExists([]byte(bucket2))
		for key := range keys {
			val := b.Get([]byte(key))
			if val != nil {
				numMovedKeys++
				b.Delete([]byte(key))
				b2.Put([]byte(key), val)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Sprintf("Error: '%s'", err.Error()), false
	}
	return fmt.Sprintf("Moved %d keys from %s to %s", numMovedKeys, bucket1, bucket2), true
}
