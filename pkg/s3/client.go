package s3

import "github.com/golang/glog"

type s3Client interface {
	Config() *Config
	BucketExists(bucketName string) (bool, error)
	CreateBucket(bucketName string) error
	CreatePrefix(bucketName string, prefix string) error
	RemovePrefix(bucketName string, prefix string) error
	RemoveBucket(bucketName string) error
}

// Config holds values to configure the driver
type Config struct {
	AccessKeyID     string
	SecretAccessKey string
	Region          string
	Endpoint        string

	AwsRoleArn string

	Mounter string
}

func NewClientFromSecret(secret map[string]string) (s3Client, error) {
	cfg := &Config{
		AccessKeyID:     secret["accessKeyID"],
		SecretAccessKey: secret["secretAccessKey"],
		Region:          secret["region"],
		Endpoint:        secret["endpoint"],
		AwsRoleArn:      secret["awsRoleArn"],
		// Mounter is set in the volume preferences, not secrets
		Mounter: "",
	}

	// If access key ID is not provided, try aws role arn.
	if cfg.AccessKeyID == "" {
		glog.Infof("NewClientFromSecret: awsRoleArn: '%s'", cfg.AwsRoleArn)
		return NewClientAws(cfg)
	}

	return NewClientMinio(cfg)
}
