package main

import (
	"flag"
	"fmt"
	"log"
	"os"
)

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
