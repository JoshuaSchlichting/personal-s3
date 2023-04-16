package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func NewS3Bucket(bucketName string) *Bucket {
	sdkConfig, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		panic(err)
	}
	s3Client := s3.NewFromConfig(sdkConfig)

	return &Bucket{
		Client:     s3Client,
		bucketName: bucketName,
	}
}

func ListBuckets() {
	sdkConfig, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		fmt.Println("Couldn't load default configuration. Have you set up your AWS account?")
		fmt.Println(err)
		return
	}
	s3Client := s3.NewFromConfig(sdkConfig)
	count := 10
	fmt.Printf("Let's list up to %v buckets for your account.\n", count)
	result, err := s3Client.ListBuckets(context.TODO(), &s3.ListBucketsInput{})
	if err != nil {
		fmt.Printf("Couldn't list buckets for your account. Here's why: %v\n", err)
		return
	}
	if len(result.Buckets) == 0 {
		fmt.Println("You don't have any buckets!")
	} else {
		for _, bucket := range result.Buckets[:count] {
			fmt.Printf("\t%v\n", *bucket.Name)
		}
	}
}

type Bucket struct {
	Client     *s3.Client
	bucketName string
}

func (b *Bucket) Name() string {
	return b.bucketName
}

func (b *Bucket) DeleteAllFiles(prefix string) error {
	if prefix == "" {
		log.Printf("Prefix is empty. Not deleting any files")
		return nil
	}
	objects, err := b.ListObjects()
	if err != nil {
		fmt.Println(err)
		return err
	}
	for _, object := range objects {
		if strings.HasPrefix(*object.Key, prefix) {
			fmt.Println("Deleting: ", *object.Key)
			b.DeleteFile(*object.Key)
		}
	}
	return nil
}

func (b *Bucket) DeleteFile(objectName string) {
	_, err := b.Client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(b.bucketName),
		Key:    aws.String(objectName),
	})
	if err != nil {
		log.Printf("Couldn't delete object %v from bucket %v. Here's why: %v\n", objectName, b.bucketName, err)
	}
}

func (b *Bucket) ListObjects() ([]s3types.Object, error) {
	p := s3.NewListObjectsV2Paginator(b.Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(b.bucketName),
	})
	var results []s3types.Object
	for p.HasMorePages() {
		page, err := p.NextPage(context.Background())
		if err != nil {
			log.Printf("Couldn't list objects in bucket %v: %v\n", b.bucketName, err)
			return nil, err
		}
		results = append(results, page.Contents...)
	}
	return results, nil
}

func (b *Bucket) GetObjectsInBucket(bucketName string) (objectNames []string) {

	objects, error := b.ListObjects()
	if error != nil {
		fmt.Println(error)
	}
	for _, object := range objects {
		fmt.Println(*object.Key)
	}
	for _, object := range objects {
		objectNames = append(objectNames, *object.Key)
	}
	return objectNames
}

func (b *Bucket) PrintObjectsInBucket(bucketName string) {
	objects, error := b.ListObjects()
	if error != nil {
		fmt.Println(error)
	}
	for _, object := range objects {
		fmt.Println(*object.Key)
	}
}

func (b *Bucket) DeleteAllFilesWhereNameContains(target string) {
	if strings.TrimSpace(target) == "" {
		log.Println("Target is empty. Not deleting any files")
		return
	}
	if target == "/" {
		log.Println("Target is root. Not deleting any files")
		return
	}
	objects, error := b.ListObjects()
	if error != nil {
		log.Fatal(error)
	}
	for _, object := range objects {
		if strings.Contains(*object.Key, target) {
			log.Println("Deleting: ", *object.Key)
			b.DeleteFile(*object.Key)
		}
	}
}
