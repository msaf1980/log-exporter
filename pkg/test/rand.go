package test

import (
	"math/rand"

	"github.com/msaf1980/go-stringutils"
)

const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 "

func Int(min, max int) int {
	return rand.Intn(max-min) + min
}

func String(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[Int(0, len(charset))]
	}
	return stringutils.UnsafeString(b)
}

func Strings(length, n int) []string {
	strs := make([]string, n)
	for i := range strs {
		strs[i] = String(length)
	}
	return strs
}
