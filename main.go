package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type s3ClientWrapper struct {
	s3Client *s3.Client
	cache    map[string]struct{}
}

func newS3ClientWrapper() (*s3ClientWrapper, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %v", err)
	}
	s3Client := s3.NewFromConfig(cfg)

	return &s3ClientWrapper{
		s3Client: s3Client,
	}, nil
}

func (c *s3ClientWrapper) SyncFolderToBucket(bucketName string, dir string) error {

	// Get list of files in folder
	files, err := ListDir(dir)
	if err != nil {
		return fmt.Errorf("failed to list files in folder: %v", err)
	}

	relativeFileNames := make([]string, len(files))
	// get filenames relative to the target directory
	for i, file := range files {
		relativeFileNames[i] = file[len(dir)-len(filepath.Base(dir)):]
	}

	// Upload files to S3 bucket
	for j, file := range relativeFileNames {
		exists, err := c.ObjectExists(bucketName, file)
		if err != nil {
			log.Printf("failed to check if object exists: %v", err)
			return err
		}
		if exists {
			log.Printf("object %s already exists in bucket %s, skipping", file, bucketName)
			continue
		}
		log.Print("uploading file ", file, " to bucket", bucketName)
		err = UploadFileToBucket(c.s3Client, bucketName, files[j])
		log.Print("uploaded complete:", file)
		if err != nil {
			log.Printf("failed to upload file %s: %v", file, err)
		}
	}

	return nil
}

func (c *s3ClientWrapper) ObjectExists(bucketName string, key string) (bool, error) {
	_, err := c.s3Client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: &bucketName,
		Key:    &key,
	})
	if err != nil {
		if strings.Contains(err.Error(), "NotFound") {
			return false, nil
		}
		return false, fmt.Errorf("failed to check if object exists: %v", err)
	}

	return true, nil
}

func ListDir(dir string) ([]string, error) {
	var files []string

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to walk folder: %v", err)
	}

	return files, nil
}

func UploadFileToBucket(s3Client *s3.Client, bucketName string, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %v",
			filePath, err)
	}
	defer file.Close()

	_, err = s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: &bucketName,
		Key:    &filePath,
		Body:   file,
	})
	if err != nil {
		return fmt.Errorf("failed to put object: %v", err)
	}

	return nil
}

func main() {
	// Parse command line flags
	bucketNamePtr := flag.String("bucket", "", "name of the S3 bucket")
	syncDirPtr := flag.String("dir", "", "directory to sync to S3 bucket")
	flag.Parse()

	// Check that the bucket name is specified
	if *bucketNamePtr == "" {
		fmt.Println("Please specify the name of the S3 bucket using the -bucket flag.")
		os.Exit(1)
	}

	s3Wrapper, err := newS3ClientWrapper()
	if err != nil {
		log.Fatalf("failed to create S3 client wrapper: %v", err)
	}

	// Sync folder to S3 bucket
	err = s3Wrapper.SyncFolderToBucket(*bucketNamePtr, *syncDirPtr)
	if err != nil {
		log.Fatalf("failed to sync folder to S3 bucket: %v", err)
	}

	fmt.Println("Sync complete!")
}
