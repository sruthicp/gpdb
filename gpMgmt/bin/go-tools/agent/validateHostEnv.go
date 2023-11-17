package agent

import (
	"fmt"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"syscall"
)

var CheckDirEmpty = CheckDirEmptyFn

func (s *Server) ValidateHostEnvFn(dirList []string, forced bool) error {
	gplog.Debug("Starting ValidateHostEnvFn for DIrList:%v", dirList)
	// Check if user is non root
	if os.Getuid() == 0 {
		userInfo, err := user.Current()
		if err != nil {
			gplog.Error("failed to get user name Error:%v. Current user is a root user. Can't create cluster under root", err)
			return fmt.Errorf("failed to get user name Error:%v. Current user is a root user. Can't create cluster under root", err)
		}
		return fmt.Errorf("user:%s is a root user, Can't create cluster under root user", userInfo.Name)
	}
	gplog.Debug("Done with checking user is non root. ")
	// Check for each directory, if directory is empty
	nonEmptyDirList := GetAllNonEmptyDir(dirList)
	gplog.Debug("Got the list of all non-empty directories")

	if len(nonEmptyDirList) > 0 && !forced {
		return fmt.Errorf("directory not empty:%v", nonEmptyDirList)
	}
	if forced && len(nonEmptyDirList) > 0 {
		gplog.Debug("Forced init. Deleting non-empty directories:%s", dirList)
		for _, dir := range dirList {
			err := os.RemoveAll(dir)
			if err != nil {
				return fmt.Errorf("delete not empty dir:%s, error:%v", dir, err)
			}
		}
	}

	// Validate permission to initdb ? Error will be returned upon running
	gplog.Debug("Checking initdb for permissions")
	initdbPath := filepath.Join(s.GpHome, "bin", "initdb")
	err := checkFilePermissions(initdbPath)
	if err != nil {
		return err
	}
	return nil
}

func GetAllNonEmptyDir(dirList []string) []string {
	var nonEmptyDir []string
	for _, dir := range dirList {
		isEmpty, err := CheckDirEmpty(dir)
		if err != nil {
			gplog.Error("Directory:%s Error checking if empty:%s", dir, err.Error())
			nonEmptyDir = append(nonEmptyDir, dir)
		}
		if !isEmpty {
			// Directory not empty
			nonEmptyDir = append(nonEmptyDir, dir)
		}
	}
	return nonEmptyDir
}
func CheckDirEmptyFn(dirPath string) (bool, error) {
	// check if dir exists
	file, err := os.Open(dirPath)
	if os.IsNotExist(err) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("error opening file:%v", err)
	}
	defer file.Close()
	_, err = file.Readdirnames(1)
	if err == io.EOF {
		return true, nil
	}
	return false, nil
}

func checkFilePermissions(filePath string) error {
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return fmt.Errorf("error getting file info:%v", err)
	}
	// Get current user-id, group-id and checks against initdb file
	err = checkFileOwnerGroup(filePath, fileInfo)
	if err != nil {
		return err
	}

	// Check if the file has execute permission
	if !checkExecutable(fileInfo.Mode()) {
		return fmt.Errorf("file %s does not have execute permissions", filePath)
	}
	return nil
}

func checkFileOwnerGroup(filePath string, fileInfo os.FileInfo) error {
	systemUid := os.Geteuid()
	systemGid := os.Getgid()
	// Fetch file info: file owner, group ID
	stat, ok := fileInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("error converting fileinfo:%v", ok)
	}

	if int(stat.Uid) != systemUid && int(stat.Gid) != systemGid {
		return fmt.Errorf("file %s is neither owned by the user nor by group", filePath)
	}
	return nil
}

func checkExecutable(FileMode os.FileMode) bool {
	return FileMode&0111 != 0
}
