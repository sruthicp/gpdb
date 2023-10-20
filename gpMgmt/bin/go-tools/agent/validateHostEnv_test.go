package agent_test

import (
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpdb/gp/agent"
	"os"
	"testing"
)

func TestCheckEmptyDir(t *testing.T) {
	testhelper.SetupTestLogger()
	t.Run("CheckemptyDir returns correct value if directory exists", func(t *testing.T) {
		testDir := "/tmp/test1"
		err := os.Mkdir(testDir, 0766)
		defer os.Remove(testDir)
		if err != nil {
			t.Fatalf("error creating test dir:%s. Error:%v", testDir, err)
		}
		isEmpty, err := agent.CheckDirEmpty(testDir)
		if err != nil {
			t.Fatalf("Got: %v, expected no error", err)
		}
		if isEmpty != true {
			t.Fatalf("expected directory empty, but its not")
		}
	})
	t.Run("CheckemptyDir returns correct value if directory does not exists", func(t *testing.T) {
		testDir := "/tmp/test1"
		isEmpty, err := agent.CheckDirEmpty(testDir)
		if err != nil {
			t.Fatalf("Got: %v, expecte no error", err)
		}
		if isEmpty != true {
			t.Fatalf("expected directory empty, but its not")
		}
	})
	t.Run("CheckemptyDir returns correct value if directory exists and non-empty", func(t *testing.T) {
		testDir := "/tmp/test1"
		testFile := "/tmp/test1/testfile"
		err := os.Mkdir(testDir, 0766)
		defer os.Remove(testDir)
		if err != nil {
			t.Fatalf("error creating test dir:%s. Error:%v", testDir, err)
		}

		file, err := os.Create(testFile)
		if err != nil {
			t.Fatalf("error creating test file:%s. Error:%v", testFile, err)
		}
		file.Close()
		defer os.Remove(testFile)
		isEmpty, err := agent.CheckDirEmpty(testDir)
		if err != nil {
			t.Fatalf("Got: %v, expected no error", err)
		}
		if isEmpty != false {
			t.Fatalf("expected directory non-empty, but returned empty")
		}
	})
}
func resetAgentFunctions() {
	agent.CheckDirEmpty = agent.CheckDirEmptyFn
}

func TestGetAllNonEmptyDir(t *testing.T) {
	testhelper.SetupTestLogger()
	t.Run("function returns list of all non empty directories", func(t *testing.T) {
		var dirList []string
		testString := "/tmp/1"
		expectedStr := "/tmp/2"
		dirList = append(dirList, testString)
		dirList = append(dirList, expectedStr)
		defer resetAgentFunctions()
		agent.CheckDirEmpty = func(dirPath string) (bool, error) {
			if dirPath == testString {
				return true, nil
			}
			return false, nil
		}
		emptyList := agent.GetAllNonEmptyDir(dirList)
		if len(emptyList) != 1 || emptyList[0] != expectedStr {
			t.Fatalf("got %q, want %q", emptyList, expectedStr)
		}
	})

}
