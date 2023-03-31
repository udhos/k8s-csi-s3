package mounter

import (
	"fmt"
	"os"
	"time"

	systemd "github.com/coreos/go-systemd/v22/dbus"
	dbus "github.com/godbus/dbus/v5"
	"github.com/golang/glog"

	"github.com/yandex-cloud/k8s-csi-s3/pkg/s3"
)

const (
	geesefsCmd = "geesefs"
)

// Implements Mounter
type geesefsMounter struct {
	meta            *s3.FSMeta
	endpoint        string
	region          string
	accessKeyID     string
	secretAccessKey string
	roleArn         string
}

func newGeeseFSMounter(meta *s3.FSMeta, cfg *s3.Config) (Mounter, error) {
	return &geesefsMounter{
		meta:            meta,
		endpoint:        cfg.Endpoint,
		region:          cfg.Region,
		accessKeyID:     cfg.AccessKeyID,
		secretAccessKey: cfg.SecretAccessKey,
		roleArn:         cfg.AwsRoleArn,
	}, nil
}

func (geesefs *geesefsMounter) CopyBinary(from, to string) error {
	st, err := os.Stat(from)
	if err != nil {
		return fmt.Errorf("Failed to stat %s: %v", from, err)
	}
	st2, err := os.Stat(to)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("Failed to stat %s: %v", to, err)
	}
	if err != nil || st2.Size() != st.Size() || st2.ModTime() != st.ModTime() {
		if err == nil {
			// remove the file first to not hit "text file busy" errors
			err = os.Remove(to)
			if err != nil {
				return fmt.Errorf("Error removing %s to update it: %v", to, err)
			}
		}
		bin, err := os.ReadFile(from)
		if err != nil {
			return fmt.Errorf("Error copying %s to %s: %v", from, to, err)
		}
		err = os.WriteFile(to, bin, 0755)
		if err != nil {
			return fmt.Errorf("Error copying %s to %s: %v", from, to, err)
		}
		err = os.Chtimes(to, st.ModTime(), st.ModTime())
		if err != nil {
			return fmt.Errorf("Error copying %s to %s: %v", from, to, err)
		}
	}
	return nil
}

func (geesefs *geesefsMounter) MountDirect(target string, args []string) error {
	args = append([]string{
		"--endpoint", geesefs.endpoint,
		"-o", "allow_other",
		"--log-file", "/dev/stderr",
	}, args...)

	creds := getCredentials(geesefs.region, geesefs.accessKeyID, geesefs.secretAccessKey, geesefs.roleArn)
	for k, v := range creds {
		os.Setenv(k, v)
	}

	return fuseMount(target, geesefsCmd, args)
}

func (geesefs *geesefsMounter) Mount(target, volumeID string) error {
	fullPath := fmt.Sprintf("%s:%s", geesefs.meta.BucketName, geesefs.meta.Prefix)
	var args []string
	if geesefs.region != "" {
		args = append(args, "--region", geesefs.region)
	}
	args = append(
		args,
		"--setuid", "65534", // nobody. drop root privileges
		"--setgid", "65534", // nogroup
	)
	useSystemd := true
	for i := 0; i < len(geesefs.meta.MountOptions); i++ {
		if geesefs.meta.MountOptions[i] == "--no-systemd" {
			useSystemd = false
		} else {
			args = append(args, geesefs.meta.MountOptions[i])
		}
	}
	args = append(args, fullPath, target)
	// Try to start geesefs using systemd so it doesn't get killed when the container exits
	if !useSystemd {
		return geesefs.MountDirect(target, args)
	}
	conn, err := systemd.New()
	if err != nil {
		glog.Errorf("Failed to connect to systemd dbus service: %v, starting geesefs directly", err)
		return geesefs.MountDirect(target, args)
	}
	defer conn.Close()
	// systemd is present
	if err = geesefs.CopyBinary("/usr/bin/geesefs", "/csi/geesefs"); err != nil {
		return err
	}
	pluginDir := os.Getenv("PLUGIN_DIR")
	if pluginDir == "" {
		pluginDir = "/var/lib/kubelet/plugins/ru.yandex.s3.csi"
	}
	args = append([]string{pluginDir + "/geesefs", "-f", "-o", "allow_other", "--endpoint", geesefs.endpoint}, args...)
	unitName := "geesefs-" + systemd.PathBusEscape(volumeID) + ".service"

	creds := getCredentials(geesefs.region, geesefs.accessKeyID, geesefs.secretAccessKey, geesefs.roleArn)
	var env []string
	for k, v := range creds {
		env = append(env, k+"="+v)
	}

	newProps := []systemd.Property{
		systemd.Property{
			Name:  "Description",
			Value: dbus.MakeVariant("GeeseFS mount for Kubernetes volume " + volumeID),
		},
		systemd.PropExecStart(args, false),
		systemd.Property{
			Name:  "Environment",
			Value: dbus.MakeVariant(env),
		},
		systemd.Property{
			Name:  "CollectMode",
			Value: dbus.MakeVariant("inactive-or-failed"),
		},
	}
	unitProps, err := conn.GetAllProperties(unitName)
	if err == nil {
		// Unit already exists
		if s, ok := unitProps["ActiveState"].(string); ok && (s == "active" || s == "activating" || s == "reloading") {
			// Unit is already active
			curPath := ""
			prevExec, ok := unitProps["ExecStart"].([][]interface{})
			if ok && len(prevExec) > 0 && len(prevExec[0]) >= 2 {
				execArgs, ok := prevExec[0][1].([]string)
				if ok && len(execArgs) >= 2 {
					curPath = execArgs[len(execArgs)-1]
				}
			}
			if curPath != target {
				return fmt.Errorf(
					"GeeseFS for volume %v is already mounted on host, but"+
						" in a different directory. We want %v, but it's in %v",
					volumeID, target, curPath,
				)
			}
			// Already mounted at right location
			return nil
		} else {
			// Stop and garbage collect the unit if automatic collection didn't work for some reason
			conn.StopUnit(unitName, "replace", nil)
			conn.ResetFailedUnit(unitName)
		}
	}
	_, err = conn.StartTransientUnit(unitName, "replace", newProps, nil)
	if err != nil {
		return fmt.Errorf("Error starting systemd unit %s on host: %v", unitName, err)
	}
	return waitForMount(target, 10*time.Second)
}
