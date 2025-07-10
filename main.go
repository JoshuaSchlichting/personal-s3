package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func main() {
	// Parse command line flags
	bucketName := flag.String("bucket", "", "name of the S3 bucket")
	syncDir := flag.String("dir", "", "directory to sync to S3 bucket")
	overwriteExistingFilesInBucket := flag.Bool("overwrite", false, "overwrite files that already exist in the S3 bucket")
	tier := flag.String("tier", "STANDARD", "storage tier for S3 objects (e.g., STANDARD, INTELLIGENT_TIERING)")
	flag.Parse()

	// Check that the bucket name is specified
	if *bucketName == "" {
		fmt.Println("Please specify the name of the S3 bucket using the -bucket flag.")
		os.Exit(1)
	}

	// Map tier argument to S3 StorageClass
	var storageClass types.StorageClass
	switch *tier {
	case "STANDARD":
		storageClass = types.StorageClassStandard
	case "INTELLIGENT_TIERING":
		storageClass = types.StorageClassIntelligentTiering
	default:
		fmt.Printf("Invalid tier specified: %s. Valid options are STANDARD or INTELLIGENT_TIERING.\n", *tier)
		os.Exit(1)
	}

	s3Wrapper, err := newS3ClientWrapper(*bucketName)
	if err != nil {
		log.Fatalf("failed to create S3 client wrapper: %v", err)
	}

	// Sync folder to S3 bucket
	err = s3Wrapper.SyncFolderToBucket(*bucketName, *syncDir, *overwriteExistingFilesInBucket, storageClass)
	if err != nil {
		log.Fatalf("failed to sync folder to S3 bucket: %v", err)
	}

	fmt.Println("Sync complete!")
}
