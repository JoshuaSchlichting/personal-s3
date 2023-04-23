package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
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

			err = UploadFileToBucket(c.s3Client, bucketName, files[j], file)
			if err != nil {
				log.Printf("failed to upload file %s: %v", file, err)
				panic(err)
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
		err = UploadFileToBucket(c.s3Client, bucketName, files[j], file)
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
	partSizeMB           = 5
	partSize             = partSizeMB * 1024 * 1024
	maxConcurrentUploads = 8
)

type eTagTracker struct {
	PartNumber int
	Etag       string
}

func UploadFileToBucket(s3Client *s3.Client, bucketName, filePath, key string) error {
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
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to create multipart upload: %v", err)
	}

	uploadID := createResp.UploadId
	fmt.Printf("Upload ID: %s\n", *uploadID)

	// Calculate the number of parts to split the file into
	numParts := int(fileSize/partSize) + 1
	log.Printf("Splitting file into %d parts of size %dMB each\n", numParts, partSizeMB)
	log.Println("Concurrent Uploads: ", maxConcurrentUploads)
	// Initialize the slice to store the ETags of the uploaded parts
	var partETags []eTagTracker

	// Create a channel to receive part ETags
	etagChan := make(chan eTagTracker, numParts)

	// Launch a Go routine for each part to upload it concurrently
	fmt.Printf("Uploading file '%s' to bucket '%s'\n", filePath, bucketName)
	fmt.Printf("Upload status: 0%%")
	startTime := time.Now()
	// Use a channel to limit the number of concurrent uploads
	sem := make(chan struct{}, maxConcurrentUploads)
	wg := sync.WaitGroup{}
	var completedParts int = 0
	for i := 1; i <= numParts; i++ {
		wg.Add(1)
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
		sem <- struct{}{}
		// Launch a Go routine to upload the part
		go func(partNumber int) {
			defer func() { <-sem }()
			defer wg.Done()

			// Upload the part
			uploadResp, err := s3Client.UploadPart(context.TODO(), &s3.UploadPartInput{
				Bucket:     &bucketName,
				Key:        aws.String(key),
				UploadId:   uploadID,
				PartNumber: *aws.Int32(int32(partNumber)),
				Body:       bytes.NewReader(partData),
			})
			if err != nil {
				log.Fatalf("failed to upload part: %v", err)
			}
			completedParts++
			// Send the ETag to the channel
			etagChan <- eTagTracker{
				Etag:       *uploadResp.ETag,
				PartNumber: partNumber,
			}

			percentage := (completedParts * 100) / numParts
			fmt.Printf("\rUpload status: %d%%", percentage)
			if percentage > 0 {
				elapsedTime := time.Since(startTime)
				uploadSpeed := float64(completedParts*partSizeMB) / elapsedTime.Seconds()
				fmt.Printf(" Current upload speed: %.2f MB/s ", uploadSpeed)
			}
		}(i)
	}
	// Wait for all uploads to complete
	wg.Wait()
	fmt.Printf("\rUpload status: 100%%                 \n")
	fmt.Println("All parts uploaded at this point. Waiting for completion...")
	// Collect the ETags of the uploaded parts
	for i := 1; i <= numParts; i++ {
		partETags = append(partETags, <-etagChan)
	}
	// Wait for all uploads to complete before completing the multipart upload
	for i := 0; i < maxConcurrentUploads; i++ {
		sem <- struct{}{}
	}
	log.Println("Multipart upload completed. Completing multipart upload in cloud storage...")
	// Complete the multipart upload
	_, err = s3Client.CompleteMultipartUpload(context.TODO(), &s3.CompleteMultipartUploadInput{
		Bucket:          &bucketName,
		Key:             aws.String(key),
		UploadId:        uploadID,
		MultipartUpload: &s3types.CompletedMultipartUpload{Parts: buildCompleteMultipartUploadParts(numParts, partETags)},
	})
	log.Println("Multipart upload completed.")
	if err != nil {
		return fmt.Errorf("failed to complete multipart upload: %v", err)
	}
	// Print the upload details
	fmt.Printf("\nSuccessfully uploaded file '%s' to bucket '%s'\n", filePath, bucketName)
	return nil
}

func buildCompleteMultipartUploadParts(numParts int, partETags []eTagTracker) []s3types.CompletedPart {
	var parts []s3types.CompletedPart

	// sort partETags by part number
	sort.Slice(partETags, func(i, j int) bool {
		return partETags[i].PartNumber < partETags[j].PartNumber
	})

	for i := 1; i <= numParts; i++ {
		parts = append(parts, s3types.CompletedPart{
			ETag:       aws.String(partETags[i-1].Etag),
			PartNumber: *aws.Int32(int32(partETags[i-1].PartNumber)),
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
