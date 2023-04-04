package mounter

import (
	"fmt"
	"os"

	"github.com/yandex-cloud/k8s-csi-s3/pkg/s3"
)

// Implements Mounter
type s3fsMounter struct {
	meta          *s3.FSMeta
	url           string
	region        string
	pwFileContent string
	roleArn       string
}

const (
	s3fsCmd = "s3fs"
)

func newS3fsMounter(meta *s3.FSMeta, cfg *s3.Config) (Mounter, error) {
	return &s3fsMounter{
		meta:          meta,
		url:           cfg.Endpoint,
		region:        cfg.Region,
		pwFileContent: cfg.AccessKeyID + ":" + cfg.SecretAccessKey,
		roleArn:       cfg.AwsRoleArn,
	}, nil
}

func (s3fs *s3fsMounter) Mount(target, volumeID string) error {
	useRole := s3fs.pwFileContent[0] == ':' // access key ID is empty
	if !useRole {
		if err := writes3fsPass(s3fs.pwFileContent); err != nil {
			return err
		}
	}
	// s3fs mybucket /path/to/mountpoint -o passwd_file=${HOME}/.passwd-s3fs
	args := []string{
		fmt.Sprintf("%s:/%s", s3fs.meta.BucketName, s3fs.meta.Prefix),
		target,
		"-o", "allow_other",
		"-o", "mp_umask=000",
	}
	if useRole {
		args = append(args, "-o", fmt.Sprintf("iam_role=%s", s3fs.roleArn))
	} else {
		args = append(args, "-o", "use_path_request_style")
		args = append(args, "-o", fmt.Sprintf("url=%s", s3fs.url))
		if s3fs.region != "" {
			args = append(args, "-o", fmt.Sprintf("endpoint=%s", s3fs.region))
		}
	}
	args = append(args, s3fs.meta.MountOptions...)
	return fuseMount(target, s3fsCmd, args)
}

func writes3fsPass(pwFileContent string) error {
	pwFileName := fmt.Sprintf("%s/.passwd-s3fs", os.Getenv("HOME"))
	pwFile, err := os.OpenFile(pwFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	_, err = pwFile.WriteString(pwFileContent)
	if err != nil {
		return err
	}
	pwFile.Close()
	return nil
}
