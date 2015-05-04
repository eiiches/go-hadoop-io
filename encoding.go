package hadoop

import "io"
import "encoding/binary"

func ReadByte(r io.Reader) (byte, error) {
	var buf [1]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return buf[0], nil
}

func DecodeVIntSize(value byte) int {
	if value <= 127 || 144 <= value {
		return 1
	} else if 127 < value && value < 136 {
		return -int(value) + 137
	} else {
		return -int(value) + 145
	}
}

func IsNegativeVInt(fst byte) bool {
	return (127 < fst && fst < 136) || (144 <= fst)
}

func ReadVLong(r io.Reader) (int64, error) {
	fst, err := ReadByte(r)
	if err != nil {
		return 0, err
	}
	len := DecodeVIntSize(fst)
	if len == 1 {
		return int64(fst), nil
	}
	var result int64 = 0
	for idx := 0; idx < len-1; idx++ {
		b, err := ReadByte(r)
		if err != nil {
			return 0, err
		}
		result = result << 8
		result = result | int64(b&0xff)
	}
	if IsNegativeVInt(fst) {
		return result ^ -1, nil
	} else {
		return result, nil
	}
}

func ReadBoolean(r io.Reader) (bool, error) {
	b, err := ReadByte(r)
	if err != nil {
		return false, err
	}
	return b != 0, nil
}

func ReadInt(r io.Reader) (int32, error) {
	var result int32
	if err := binary.Read(r, binary.BigEndian, &result); err != nil {
		return 0, err
	}
	return result, nil
}

func ReadBuffer(r io.Reader) ([]byte, error) {
	size, err := ReadVLong(r)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
