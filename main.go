package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type s3ClientWrapper struct {
	s3Client *s3.Client
	cache    S3Cache
}

func newS3ClientWrapper(bucketName string) (*s3ClientWrapper, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %v", err)
	}
	s3Client := s3.NewFromConfig(cfg)

	cache, err := loadCache(bucketName)
	if err != nil {
		return nil, fmt.Errorf("failed to load cache: %v", err)
	}
	return &s3ClientWrapper{
		s3Client: s3Client,
		cache:    *cache,
	}, nil
}

func (c *s3ClientWrapper) SyncFolderToBucket(bucketName string, dir string, useCache bool) error {

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
		if useCache {
			if c.cache.isCached(file) {
				log.Printf("object %s already exists in bucket %s, skipping", file, bucketName)
				continue
			}
			log.Println("uploading file ", files[j], " to bucket: ", bucketName)
			err = UploadFileToBucket(c.s3Client, bucketName, files[j])
			if err != nil {
				log.Printf("failed to upload file %s: %v", file, err)
			} else {
				log.Print("upload complete:", file)
				c.cache.cache[file] = struct{}{}
				c.cache.saveCache()
			}
			continue
		}

		exists, err := c.ObjectExists(bucketName, file)
		if err != nil {
			log.Printf("failed to check if object exists: %v", err)
			return err
		}
		if exists {
			log.Printf("object %s already exists in bucket %s, skipping", file, bucketName)
			continue
		}
		log.Print("uploading file ", file, " to bucket ", bucketName)
		err = UploadFileToBucket(c.s3Client, bucketName, files[j])
		if err != nil {
			log.Printf("failed to upload file %s: %v", file, err)
		} else {
			log.Print("upload complete:", file)
			c.cache.cache[file] = struct{}{}
			c.cache.saveCache()
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

const (
	// Define the part size in MB
	partSize = 5 * 1024 * 1024
)

func UploadFileToBucket(s3Client *s3.Client, bucketName string, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %v", filePath, err)
	}
	defer file.Close()

	// Get the file size
	fileInfo, _ := file.Stat()
	fileSize := fileInfo.Size()

	// Create the multipart upload
	createResp, err := s3Client.CreateMultipartUpload(context.TODO(), &s3.CreateMultipartUploadInput{
		Bucket: &bucketName,
		Key:    aws.String(filepath.Base(filePath)),
	})
	if err != nil {
		return fmt.Errorf("failed to create multipart upload: %v", err)
	}

	uploadID := createResp.UploadId
	fmt.Printf("Upload ID: %s\n", *uploadID)

	// Calculate the number of parts to split the file into
	numParts := int(fileSize/partSize) + 1

	// Initialize the slice to store the ETags of the uploaded parts
	var partETags []string

	fmt.Printf("Uploading file '%s' to bucket '%s'\n", filePath, bucketName)
	fmt.Printf("Upload status: 0%%")
	startTime := time.Now()
	for i := 1; i <= numParts; i++ {
		partStart := int64((i - 1) * partSize)
		partEnd := int64(i * partSize)
		if partEnd > fileSize {
			partEnd = fileSize
		}

		// Read the part data
		partData := make([]byte, partEnd-partStart)
		_, err := file.ReadAt(partData, partStart)
		if err != nil {
			return fmt.Errorf("failed to read file part: %v", err)
		}

		// Upload the part
		uploadResp, err := s3Client.UploadPart(context.TODO(), &s3.UploadPartInput{
			Bucket:     &bucketName,
			Key:        aws.String(filepath.Base(filePath)),
			UploadId:   uploadID,
			PartNumber: *aws.Int32(int32(i)),
			Body:       bytes.NewReader(partData),
		})
		if err != nil {
			return fmt.Errorf("failed to upload part: %v", err)
		}

		partETags = append(partETags, *uploadResp.ETag)
		percentage := (i * 100) / numParts
		fmt.Printf("\rUpload status: %d%%", percentage)
		if percentage > 0 {
			elapsedTime := time.Since(startTime)
			uploadSpeed := float64(partEnd-partStart) / elapsedTime.Seconds() / 1024 / 1024
			fmt.Printf(" Upload speed: %.2f MB/s", uploadSpeed)
		}
	}

	// Complete the multipart upload
	_, err = s3Client.CompleteMultipartUpload(context.TODO(), &s3.CompleteMultipartUploadInput{
		Bucket:   &bucketName,
		Key:      aws.String(filepath.Base(filePath)),
		UploadId: uploadID,
		MultipartUpload: &s3types.CompletedMultipartUpload{
			Parts: buildCompleteMultipartUploadParts(numParts, partETags),
		},
	})
	if err != nil {
		return fmt.Errorf("failed to complete multipart upload: %v", err)
	}

	fmt.Println("File uploaded successfully")

	return nil
}

func buildCompleteMultipartUploadParts(numParts int, partETags []string) []s3types.CompletedPart {
	var parts []s3types.CompletedPart
	for i := 1; i <= numParts; i++ {
		parts = append(parts, s3types.CompletedPart{
			ETag:       aws.String(partETags[i-1]),
			PartNumber: *aws.Int32(int32(i)),
		})
	}
	return parts
}

func main() {
	// Parse command line flags
	bucketNamePtr := flag.String("bucket", "", "name of the S3 bucket")
	syncDirPtr := flag.String("dir", "", "directory to sync to S3 bucket")
	useCache := flag.Bool("cache", false, "use cache to skip files that have already been uploaded in lieu of HEAD-OBJECT calls to AWS")
	flag.Parse()

	// Check that the bucket name is specified
	if *bucketNamePtr == "" {
		fmt.Println("Please specify the name of the S3 bucket using the -bucket flag.")
		os.Exit(1)
	}

	s3Wrapper, err := newS3ClientWrapper(*bucketNamePtr)
	if err != nil {
		log.Fatalf("failed to create S3 client wrapper: %v", err)
	}

	// Sync folder to S3 bucket
	err = s3Wrapper.SyncFolderToBucket(*bucketNamePtr, *syncDirPtr, *useCache)
	if err != nil {
		log.Fatalf("failed to sync folder to S3 bucket: %v", err)
	}

	fmt.Println("Sync complete!")
}
