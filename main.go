package main

import (
	"bufio"
	"fmt"
	"strings"
)

func main() {
	// fmt.Println("Hello World")

	// const (
	// 	BlockSize  = 4096
	// 	BufferSize = 256
	// )

	// directory := "data"
	// fileManager := file.NewManager(directory, BlockSize)
	// logManager := log.NewManager(fileManager, "log.log")
	// bufferManager := buffer.NewManager(fileManager, logManager, BufferSize)

	// tx := tx.NewTransaction(fileManager, logManager, bufferManager)

	// isNew := fileManager.IsNew()
	// if isNew {
	// 	logger.Println("creating new database...")
	// } else {
	// 	logger.Println("recovering existing database")
	// 	tx.Recover()
	// }

	// mm := metadata.NewMetadataManager(isNew, tx)
	// tx.Commit()

	source := "The quick.brown_fox jumps, over the lazy dog."

	input := strings.NewReader(source)
	scanner := bufio.NewScanner(input)
	scanner.Split(bufio.ScanWords)

	for scanner.Scan() {
		token := scanner.Text()
		fmt.Println("Token:", token)
	}
}

// package fslices
func Map[S ~[]E, D ~[]T, E any, T any](x S, f func(E) T) D {
	r := make(D, len(x))
	for i := range x {
		r[i] = f(x[i])
	}
	return r
}

func FlatMap[S ~[]E, D ~[]T, E any, T any](x S, f func(E) []T) D {
	r := make(D, 0)
	for i := range x {
		r = append(r, f(x[i])...)
	}
	return r
}

func Filter[S ~[]E, E any](x S, f func(E) bool) S {
	r := make(S, 0)
	for i := range x {
		if f(x[i]) {
			r = append(r, x[i])
		}
	}
	return r
}

func FoldLeft[S ~[]E, E any, T any](x S, i T, f func(T, E) T) T {
	acc := i
	for j := range x {
		acc = f(acc, x[j])
	}
	return acc
}
