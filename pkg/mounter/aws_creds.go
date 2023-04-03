package mounter

import (
	"context"

	"github.com/golang/glog"
	"github.com/udhos/boilerplate/awsconfig"
)

// getCredentials creates a map with credentials as env vars.
// If accessKeyID is provided, the map will contain static credentials.
// Otherwise the fuction will retrieve credentials from the roleArn.
func getCredentials(region, accessKeyID, secretAccessKey, roleArn string) map[string]string {

	if accessKeyID != "" {
		//
		// we prefer static credentials, if provided
		//
		return map[string]string{
			"AWS_ACCESS_KEY_ID":     accessKeyID,
			"AWS_SECRET_ACCESS_KEY": secretAccessKey,
		}
	}

	//
	// otherwise we will try role credentials
	//

	awsConfOptions := awsconfig.Options{
		Region:          region,
		RoleArn:         roleArn,
		RoleSessionName: "k8s-csi-s3 s3driver",
		Printf:          glog.Infof,
	}

	awsConf, errAwsConf := awsconfig.AwsConfig(awsConfOptions)
	if errAwsConf != nil {
		glog.Errorf("getCredentials: error aws config: %v", errAwsConf)
		return map[string]string{}
	}

	creds, err := awsConf.Credentials.Retrieve(context.TODO())
	if err != nil {
		glog.Errorf("getCredentials: error retrieving role credentials: %v", err)
		return map[string]string{}
	}

	return map[string]string{
		"AWS_ACCESS_KEY_ID":     creds.AccessKeyID,
		"AWS_SECRET_ACCESS_KEY": creds.SecretAccessKey,
		"AWS_SESSION_TOKEN":     creds.SessionToken,
	}
}
