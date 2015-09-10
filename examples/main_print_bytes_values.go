package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/eiiches/go-hadoop-io"
)

func WithOpenR(path string, block func(r io.Reader) error) error {
	fp, err := os.Open(path)
	if err != nil {
		return err
	}
	defer fp.Close()
	return block(fp)
}

func main() {
	flag.Parse()
	for _, arg := range flag.Args() {
		err := WithOpenR(arg, func(r io.Reader) error {
			reader, err := hadoop.NewSequenceFileReader(r)
			if err != nil {
				return err
			}
			var key hadoop.LongWritable
			var value hadoop.BytesWritable
			for {
				if err := reader.Read(&key, &value); err != nil {
					if err == io.EOF {
						break
					}
					return err
				}
				fmt.Println(string(value.Buf))
			}
			return reader.Close()
		})
		if err != nil {
			panic(err)
		}
	}
}
