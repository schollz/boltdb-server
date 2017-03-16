package main

import (
	"errors"
	"os"
	"path"

	"github.com/boltdb/bolt"
)

func getNumberKeysInBucket(dbname string, bucket string) (n int, err error) {
	n = 0
	if _, err := os.Stat(path.Join("dbs", dbname+".db")); os.IsNotExist(err) {
		return n, err
	}

	db, err := bolt.Open(path.Join("dbs", dbname+".db"), 0600, nil)
	if err != nil {
		return n, err
	}
	defer db.Close()

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
	return n, err
}

func getBucketNames(dbname string) (bucketNames []string, err error) {
	if _, err = os.Stat(path.Join("dbs", dbname+".db")); os.IsNotExist(err) {
		return bucketNames, err
	}

	db, err := bolt.Open(path.Join("dbs", dbname+".db"), 0600, nil)
	if err != nil {
		return bucketNames, err
	}
	defer db.Close()

	err = db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
			bucketNames = append(bucketNames, string(name))
			return nil
		})
	})
	return bucketNames, err
}

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

func deleteDatabase(dbname string) error {
	return os.Remove(path.Join("dbs", dbname+".db"))
}

func deleteKeys(dbname string, bucket string, keys []string) error {
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
		for _, key := range keys {
			b.Delete([]byte(key))
		}
		return err
	})
}

func deleteBucket(dbname string, bucket string) error {
	db, err := bolt.Open(path.Join("dbs", dbname+".db"), 0600, nil)
	if err != nil {
		return err
	}
	defer db.Close()

	return db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket([]byte(bucket))
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

func moveBuckets(dbname string, bucket1 string, bucket2 string, keys []string) error {
	db, err := bolt.Open(path.Join("dbs", dbname+".db"), 0600, nil)
	if err != nil {
		return err
	}
	defer db.Close()

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
			}
		}
		return nil
	})
}
