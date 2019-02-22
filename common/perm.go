package common

const (
	permPriority = 0x1 << 3
	permRead     = 0x1 << 2
	permWrite    = 0x1 << 1
	permInherit  = 0x1 << 0
)

func isReadable(perm int) bool {
	return (perm & permRead) == permRead
}

func isWriteable(perm int) bool {
	return (perm & permWrite) == permWrite
}

func isInherited(perm int) bool {
	return (perm & permInherit) == permInherit
}

func perm2string(perm int) string {
	bytes := make([]byte, 3)
	for i := 0; i < 3; i++ {
		bytes[i] = '-'
	}

	if isReadable(perm) {
		bytes[0] = 'R'
	}

	if isWriteable(perm) {
		bytes[1] = 'W'
	}

	if isInherited(perm) {
		bytes[2] = 'X'
	}

	return string(bytes)
}
