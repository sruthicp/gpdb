package assertions

import (
	"os"
	"reflect"
	"strings"
	"testing"
)

func AssertEqual(t *testing.T, expected, actual interface{}) bool {
	if expected != actual {
		t.Fatalf("Expected %#v got: %#v", expected, actual)
	}
	return true
}

func AssertEqualValues(t *testing.T, expected, actual interface{}) bool {
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("Expected %#v got: %#v", expected, actual)
	}
	return true
}

func AssertContains(t *testing.T, expected []string, actual string) bool {
	for _, item := range expected {
		if !strings.Contains(actual, item) {
			t.Fatalf("Expected string %#v not found in %#v", item, actual)
		}
	}
	return true
}

func AssertFileExists(t *testing.T, file string) bool {
	if _, err := os.Stat(file); err != nil {
		t.Fatalf("File %s not found", file)
	}
	return false
}
