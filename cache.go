package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
)

type S3Cache struct {
	cache      map[string]struct{}
	bucketName string
}

func createCacheFileIfNotExists(bucketName string) error {
	dirName := os.Getenv("HOME") + "/.personal-s3"
	if _, err := os.Stat(dirName); os.IsNotExist(err) {
		// Create directory if it doesn't exist
		err = os.Mkdir(dirName, 0755)
		if err != nil {
			fmt.Println("Error creating directory:", err)
			return err
		}
		fmt.Println("Directory created:", dirName)
	} else {
		fmt.Println("Directory already exists:", dirName)
	}
	cacheFilename := path.Join(dirName, bucketName+".json")
	if _, err := os.Stat(cacheFilename); os.IsNotExist(err) {
		file, err := os.Create(cacheFilename)
		if err != nil {
			fmt.Println("Error creating file:", err)
			return err
		}
		defer file.Close()
		file.Write([]byte("{}"))
	}
	return nil
}
func loadCache(bucketName string) (*S3Cache, error) {
	createCacheFileIfNotExists(bucketName)

	cache := make(map[string]struct{})
	cacheFilename := path.Join(os.Getenv("HOME"), ".personal-s3", bucketName+".json")
	file, err := os.Open(cacheFilename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	err = json.NewDecoder(file).Decode(&cache)
	if err != nil {
		return nil, err
	}

	result := &S3Cache{
		cache:      cache,
		bucketName: bucketName,
	}
	for key := range cache {
		result.cache[key] = struct{}{}
	}

	return result, nil
}

func (c *S3Cache) saveCache() error {
	file, err := os.OpenFile(path.Join(os.Getenv("HOME"), ".personal-s3", c.bucketName+".json"), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	err = encoder.Encode(c.cache)
	if err != nil {
		return err
	}

	return nil
}

func (c *S3Cache) isCached(key string) bool {
	_, ok := c.cache[key]
	return ok
}
