package s3

import (
	"bytes"
	"context"
	"fmt"
	"net/url"

	"github.com/golang/glog"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const (
	metadataName = ".metadata.json"
)

type s3ClientMinio struct {
	config *Config
	minio  *minio.Client
	ctx    context.Context
}

type FSMeta struct {
	BucketName    string   `json:"Name"`
	Prefix        string   `json:"Prefix"`
	Mounter       string   `json:"Mounter"`
	MountOptions  []string `json:"MountOptions"`
	CapacityBytes int64    `json:"CapacityBytes"`
}

func NewClientMinio(cfg *Config) (*s3ClientMinio, error) {
	var client = &s3ClientMinio{}

	client.config = cfg
	u, err := url.Parse(client.config.Endpoint)
	if err != nil {
		return nil, err
	}
	ssl := u.Scheme == "https"
	endpoint := u.Hostname()
	if u.Port() != "" {
		endpoint = u.Hostname() + ":" + u.Port()
	}
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(client.config.AccessKeyID, client.config.SecretAccessKey, client.config.Region),
		Secure: ssl,
	})
	if err != nil {
		return nil, err
	}
	client.minio = minioClient
	client.ctx = context.Background()
	return client, nil
}

func (client *s3ClientMinio) Config() *Config {
	return client.config
}

func (client *s3ClientMinio) BucketExists(bucketName string) (bool, error) {
	return client.minio.BucketExists(client.ctx, bucketName)
}

func (client *s3ClientMinio) CreateBucket(bucketName string) error {
	return client.minio.MakeBucket(client.ctx, bucketName, minio.MakeBucketOptions{Region: client.config.Region})
}

func (client *s3ClientMinio) CreatePrefix(bucketName string, prefix string) error {
	if prefix != "" {
		_, err := client.minio.PutObject(client.ctx, bucketName, prefix+"/", bytes.NewReader([]byte("")), 0, minio.PutObjectOptions{})
		if err != nil {
			return err
		}
	}
	return nil
}

func (client *s3ClientMinio) RemovePrefix(bucketName string, prefix string) error {
	var err error

	if err = client.removeObjects(bucketName, prefix); err == nil {
		return client.minio.RemoveObject(client.ctx, bucketName, prefix, minio.RemoveObjectOptions{})
	}

	glog.Warningf("removeObjects failed with: %s, will try removeObjectsOneByOne", err)

	if err = client.removeObjectsOneByOne(bucketName, prefix); err == nil {
		return client.minio.RemoveObject(client.ctx, bucketName, prefix, minio.RemoveObjectOptions{})
	}

	return err
}

func (client *s3ClientMinio) RemoveBucket(bucketName string) error {
	var err error

	if err = client.removeObjects(bucketName, ""); err == nil {
		return client.minio.RemoveBucket(client.ctx, bucketName)
	}

	glog.Warningf("removeObjects failed with: %s, will try removeObjectsOneByOne", err)

	if err = client.removeObjectsOneByOne(bucketName, ""); err == nil {
		return client.minio.RemoveBucket(client.ctx, bucketName)
	}

	return err
}

func (client *s3ClientMinio) removeObjects(bucketName, prefix string) error {
	objectsCh := make(chan minio.ObjectInfo)
	var listErr error

	go func() {
		defer close(objectsCh)

		for object := range client.minio.ListObjects(
			client.ctx,
			bucketName,
			minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
			if object.Err != nil {
				listErr = object.Err
				return
			}
			objectsCh <- object
		}
	}()

	if listErr != nil {
		glog.Error("Error listing objects", listErr)
		return listErr
	}

	select {
	default:
		opts := minio.RemoveObjectsOptions{
			GovernanceBypass: true,
		}
		errorCh := client.minio.RemoveObjects(client.ctx, bucketName, objectsCh, opts)
		haveErrWhenRemoveObjects := false
		for e := range errorCh {
			glog.Errorf("Failed to remove object %s, error: %s", e.ObjectName, e.Err)
			haveErrWhenRemoveObjects = true
		}
		if haveErrWhenRemoveObjects {
			return fmt.Errorf("Failed to remove all objects of bucket %s", bucketName)
		}
	}

	return nil
}

// will delete files one by one without file lock
func (client *s3ClientMinio) removeObjectsOneByOne(bucketName, prefix string) error {
	parallelism := 16
	objectsCh := make(chan minio.ObjectInfo, 1)
	guardCh := make(chan int, parallelism)
	var listErr error
	totalObjects := 0
	removeErrors := 0

	go func() {
		defer close(objectsCh)

		for object := range client.minio.ListObjects(client.ctx, bucketName,
			minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
			if object.Err != nil {
				listErr = object.Err
				return
			}
			totalObjects++
			objectsCh <- object
		}
	}()

	if listErr != nil {
		glog.Error("Error listing objects", listErr)
		return listErr
	}

	for object := range objectsCh {
		guardCh <- 1
		go func() {
			err := client.minio.RemoveObject(client.ctx, bucketName, object.Key,
				minio.RemoveObjectOptions{VersionID: object.VersionID})
			if err != nil {
				glog.Errorf("Failed to remove object %s, error: %s", object.Key, err)
				removeErrors++
			}
			<-guardCh
		}()
	}
	for i := 0; i < parallelism; i++ {
		guardCh <- 1
	}
	for i := 0; i < parallelism; i++ {
		<-guardCh
	}

	if removeErrors > 0 {
		return fmt.Errorf("Failed to remove %v objects out of total %v of path %s", removeErrors, totalObjects, bucketName)
	}

	return nil
}
