package testutils

import (
	"os"
	"reflect"
	"strings"
	"testing"
)

func Equal(t *testing.T, expected, actual interface{}) bool {
	if expected != actual {
		t.Fatalf("Expected %#v got: %#v", expected, actual)
	}
	return true
}

func NotNil(t *testing.T, actual interface{}) bool {
	if actual == nil {
		t.Fatalf("Expected error got: %#v", actual)
	}
	return true
}

func EqualValues(t *testing.T, expected, actual interface{}) bool {
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("Expected %#v got: %#v", expected, actual)
	}
	return true
}

func Contains(t *testing.T, expected []string, actual string) bool {
	for _, item := range expected {
		if !strings.Contains(actual, item) {
			t.Fatalf("Expected string %#v not found in %#v", item, actual)
		}
	}
	return true
}

func FileExists(t *testing.T, file string) bool {
	if _, err := os.Stat(file); err != nil {
		t.Fatalf("File %s not found", file)
	}
	return false
}

func ServiceFilesExist(t *testing.T, files ...string) {
	for _, file := range files {
		FileExists(t, file)
	}
}
