package hadoop

import "os"
import "fmt"
import "io"
import "testing"

func TestSequenceFile(t *testing.T) {

	fp, err := os.Open("test-bc-long-bytes.seq.bz2")
	if err != nil {
		panic(err)
	}
	defer fp.Close()
	reader, err := NewSequenceFileReader(fp)
	if err != nil {
		panic(err)
	}

	var key LongWritable
	var value BytesWritable

	for {
		if err := reader.Read(&key, &value); err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		fmt.Println(key, ": ", value)
	}

	reader.Close()

	// fmt.Println("test")
	// hio.IntWritable(10).Write(os.Stdout)

	// var value hio.IntWritable
	// value.Read(os.Stdin)
	// value.Write(os.Stdout)
	// fmt.Println(value)

	// bytes := &hio.BytesWritable{[]byte("string")}
	// bytes.Write(os.Stdout)

	// var r hio.BytesWritable
	// if err := r.Read(os.Stdin); err != nil {
	// 	fmt.Println(err)
	// }
	// fmt.Println(string(r.Buf))
}
