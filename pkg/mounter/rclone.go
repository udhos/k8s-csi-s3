package mounter

import (
	"fmt"
	"os"
	"path"

	"github.com/yandex-cloud/k8s-csi-s3/pkg/s3"
)

// Implements Mounter
type rcloneMounter struct {
	meta            *s3.FSMeta
	url             string
	region          string
	accessKeyID     string
	secretAccessKey string
	roleArn         string
}

const (
	rcloneCmd = "rclone"
)

func newRcloneMounter(meta *s3.FSMeta, cfg *s3.Config) (Mounter, error) {
	return &rcloneMounter{
		meta:            meta,
		url:             cfg.Endpoint,
		region:          cfg.Region,
		accessKeyID:     cfg.AccessKeyID,
		secretAccessKey: cfg.SecretAccessKey,
		roleArn:         cfg.AwsRoleArn,
	}, nil
}

func (rclone *rcloneMounter) Mount(target, volumeID string) error {
	args := []string{
		"mount",
		fmt.Sprintf(":s3:%s", path.Join(rclone.meta.BucketName, rclone.meta.Prefix)),
		fmt.Sprintf("%s", target),
		"--daemon",
		"--s3-provider=AWS",
		"--s3-env-auth=true",
		fmt.Sprintf("--s3-endpoint=%s", rclone.url),
		"--allow-other",
		"--vfs-cache-mode=writes",
	}
	if rclone.region != "" {
		args = append(args, fmt.Sprintf("--s3-region=%s", rclone.region))
	}
	args = append(args, rclone.meta.MountOptions...)

	creds := getCredentials(rclone.region, rclone.accessKeyID, rclone.secretAccessKey, rclone.roleArn)
	for k, v := range creds {
		os.Setenv(k, v)
	}

	return fuseMount(target, rcloneCmd, args)
}
