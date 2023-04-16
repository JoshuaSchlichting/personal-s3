package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
)

func createCache(bucketName string) error {
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
	if _, err := os.Stat(path.Join(dirName, bucketName)); os.IsNotExist(err) {
		file, err := os.Create(path.Join(dirName, bucketName))
		if err != nil {
			fmt.Println("Error creating file:", err)
			return err
		}
		defer file.Close()
		file.Write([]byte("{}"))
	}
	return nil
}
func loadCache(bucketName string) (map[string]struct{}, error) {
	cache := make(map[string]struct{})

	file, err := os.Open(path.Join(os.Getenv("HOME"), ".personal-s3", bucketName))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	err = json.NewDecoder(file).Decode(&cache)
	if err != nil {
		return nil, err
	}

	result := make(map[string]struct{})
	for key := range cache {
		result[key] = struct{}{}
	}

	return result, nil
}

func saveCache(bucketName string, cache map[string]struct{}) error {
	file, err := os.OpenFile(path.Join(os.Getenv("HOME"), ".personal-s3", bucketName), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	err = json.NewEncoder(file).Encode(cache)
	if err != nil {
		return err
	}

	return nil
}
