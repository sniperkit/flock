package store

func Combine(table string, key []byte) []byte {
	l := len(table) + len(key)
	ret := make([]byte, l)
	copy(ret, []byte(table))
	copy(ret[len(table):], key)
	return ret
}
