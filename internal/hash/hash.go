package hash

import "encoding/binary"

func HashInt(val int) uint64 {
	b := make([]byte, 4)

	binary.BigEndian.PutUint32(b, uint32(val))
	return HashKey(b)
}

func HashKey(data []byte) uint64 {
	/*
		algorithm fnv-1a is
		    hash := FNV_offset_basis

		    for each byte_of_data to be hashed do
		        hash := hash XOR byte_of_data (14695981039346656037)
		        hash := hash × FNV_prime (1099511628211)

		    return hash
	*/

	const (
		offset uint64 = 14695981039346656037
		prime         = 1099511628211
	)

	h := offset
	for i := range data {
		h = (h ^ uint64(data[i])) * prime
	}
	return h
}
