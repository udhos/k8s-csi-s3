package s3

import (
	"bytes"
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/golang/glog"
	"github.com/udhos/boilerplate/awsconfig"
)

type s3ClientAws struct {
	config      *Config
	awsS3Client *s3.Client
}

func NewClientAws(cfg *Config) (*s3ClientAws, error) {

	awsConfOptions := awsconfig.Options{
		Region:          cfg.Region,
		RoleArn:         cfg.AwsRoleArn,
		RoleSessionName: "k8s-csi-s3driver",
		Printf:          glog.Infof,
	}

	awsConf, errAwsConf := awsconfig.AwsConfig(awsConfOptions)
	if errAwsConf != nil {
		return nil, errAwsConf
	}

	client := &s3ClientAws{
		config:      cfg,
		awsS3Client: s3.NewFromConfig(awsConf),
	}

	return client, nil
}

func (client *s3ClientAws) Config() *Config {
	return client.config
}

func (client *s3ClientAws) BucketExists(bucketName string) (bool, error) {
	input := s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	}
	_, err := client.awsS3Client.HeadBucket(context.TODO(), &input)
	return err == nil, err
}

func (client *s3ClientAws) CreateBucket(bucketName string) error {
	input := s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
		CreateBucketConfiguration: &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(client.config.Region),
		},
	}
	_, err := client.awsS3Client.CreateBucket(context.TODO(), &input)
	return err
}

func (client *s3ClientAws) CreatePrefix(bucketName string, prefix string) error {
	if prefix == "" {
		return nil
	}
	input := s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(prefix + "/"),
		Body:   &bytes.Buffer{},
	}
	_, err := client.awsS3Client.PutObject(context.TODO(), &input)
	return err
}

func (client *s3ClientAws) RemovePrefix(bucketName string, prefix string) error {
	errObj := client.removeObjectsFromPrefix(bucketName, prefix)
	glog.Error("RemovePrefix: removeObjectsFromPrefix: %v", errObj)

	return client.removeObjects(bucketName, []string{prefix})
}

func (client *s3ClientAws) RemoveBucket(bucketName string) error {
	errObj := client.removeObjectsFromPrefix(bucketName, "")
	glog.Error("RemoveBucket: removeObjectsFromPrefix: %v", errObj)

	input := s3.DeleteBucketInput{Bucket: aws.String(bucketName)}
	_, err := client.awsS3Client.DeleteBucket(context.TODO(), &input)
	return err
}

func (client *s3ClientAws) removeObjectsFromPrefix(bucketName string, prefix string) error {

	result, errList := client.awsS3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(prefix),
	})

	if errList != nil {
		glog.Errorf("removeObjectsFromPrefix: bucket='%s' prefix='%s': error listing objects: %v",
			bucketName, prefix, errList)
		return errList
	}

	glog.Info("removeObjectsFromPrefix: bucket='%s' prefix='%s': found %d keys",
		bucketName, prefix, len(result.Contents))

	var keys []string
	for _, o := range result.Contents {
		keys = append(keys, *o.Key)
	}

	return client.removeObjects(bucketName, keys)
}

func (client *s3ClientAws) removeObjects(bucketName string, objectKeys []string) error {
	var objectIds []types.ObjectIdentifier
	for _, key := range objectKeys {
		objectIds = append(objectIds, types.ObjectIdentifier{Key: aws.String(key)})
	}
	input := s3.DeleteObjectsInput{
		Bucket: aws.String(bucketName),
		Delete: &types.Delete{Objects: objectIds},
	}
	_, err := client.awsS3Client.DeleteObjects(context.TODO(), &input)
	return err
}
