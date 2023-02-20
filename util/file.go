package util

import "os"

func PathExist(path string) bool {
	if _, err := os.Stat(path); err != nil {
		return false
	}
	return true
}
