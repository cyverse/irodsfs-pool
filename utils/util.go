package utils

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

func ParseTime(t string) (time.Time, error) {
	return time.Parse(time.RFC3339, t)
}

func MakeTimeToString(t time.Time) string {
	return t.Format(time.RFC3339)
}

func MakeHash(s string) string {
	hash := sha1.New()
	hash.Write([]byte(s))
	hashBytes := hash.Sum(nil)
	return hex.EncodeToString(hashBytes)
}

// JoinPath makes the path from dir and file paths
func JoinPath(dirPath string, filePath string) string {
	if strings.HasSuffix(dirPath, "/") {
		return fmt.Sprintf("%s/%s", dirPath[0:len(dirPath)-1], filePath)
	}
	return fmt.Sprintf("%s/%s", dirPath, filePath)
}
