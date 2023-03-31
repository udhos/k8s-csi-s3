package awsconfig

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/golang/glog"
)

func AwsConfig(region, roleArn, roleSessionName string) aws.Config {
	const me = "awsConfig"

	cfg, errConfig := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region))
	if errConfig != nil {
		glog.Fatalf("%s: load config: %v", me, errConfig)
	}

	if roleArn != "" {
		//
		// AssumeRole
		//
		glog.Infof("%s: AssumeRole: arn: %s", me, roleArn)
		clientSts := sts.NewFromConfig(cfg)
		cfg2, errConfig2 := config.LoadDefaultConfig(
			context.TODO(), config.WithRegion(region),
			config.WithCredentialsProvider(aws.NewCredentialsCache(
				stscreds.NewAssumeRoleProvider(
					clientSts,
					roleArn,
					func(o *stscreds.AssumeRoleOptions) {
						o.RoleSessionName = roleSessionName
					},
				)),
			),
		)
		if errConfig2 != nil {
			glog.Fatalf("%s: AssumeRole %s: error: %v", me, roleArn, errConfig2)
		}
		cfg = cfg2
	}

	{
		// show caller identity
		clientSts := sts.NewFromConfig(cfg)
		input := sts.GetCallerIdentityInput{}
		respSts, errSts := clientSts.GetCallerIdentity(context.TODO(), &input)
		if errSts != nil {
			glog.Errorf("%s: GetCallerIdentity: error: %v", me, errSts)
		} else {
			glog.Infof("%s: GetCallerIdentity: Account=%s ARN=%s UserId=%s", me, *respSts.Account, *respSts.Arn, *respSts.UserId)
		}
	}

	return cfg
}
