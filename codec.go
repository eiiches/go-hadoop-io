package hadoop

// #cgo LDFLAGS: -llz4
// #cgo CFLAGS: -O3
// #include "lz4.h"
import "C"

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"unsafe"

	"github.com/eiiches/go-bzip2"
)

var (
	Codecs map[string]Codec = map[string]Codec{
		"org.apache.hadoop.io.compress.Lz4Codec":   &Lz4Codec{},
		"org.apache.hadoop.io.compress.BZip2Codec": &Bzip2Codec{},
	}
)

type Codec interface {
	Uncompress(dst, src []byte) ([]byte, error)
	Compress(dst, src []byte) ([]byte, error)
}

type Bzip2Codec struct {
}

func (c *Bzip2Codec) Compress(dst, src []byte) ([]byte, error) {
	panic("")
}

func (c *Bzip2Codec) Uncompress(dst, src []byte) ([]byte, error) {
	reader, err := bzip2.NewReader(bytes.NewReader(src))
	if err != nil {
		return nil, err
	}
	var buf [512]byte
	for {
		n, err := reader.Read(buf[:])
		if err != nil && err != io.EOF {
			return nil, err
		}
		dst = append(dst, buf[:n]...)
		if err == io.EOF {
			break
		}
	}
	return dst, nil
}

type Lz4Codec struct {
}

func (c *Lz4Codec) Compress(dst, src []byte) ([]byte, error) {
	panic("")
}

func lz4DecompressSafe(in, out []byte) (int, error) {
	n := int(C.LZ4_decompress_safe((*C.char)(unsafe.Pointer(&in[0])), (*C.char)(unsafe.Pointer(&out[0])), C.int(len(in)), C.int(len(out))))
	if n < 0 {
		return 0, errors.New("corrupt input")
	}
	return n, nil
}

func (c *Lz4Codec) Uncompress(dst, src []byte) ([]byte, error) {
	var iptr, optr uint

	osize := uint(binary.BigEndian.Uint32(src[0:]))
	iptr += 4
	out := make([]byte, osize)

	for optr < osize {
		iblocksize := uint(binary.BigEndian.Uint32(src[iptr:]))
		iptr += 4

		n, err := lz4DecompressSafe(src[iptr:iptr+iblocksize], out[optr:])
		if err != nil {
			return nil, err
		}
		optr += uint(n)
		iptr += iblocksize
	}

	return out, nil
}

type SnappyCodec struct {
}
