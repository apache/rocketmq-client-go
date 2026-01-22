package utils

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFileReadAll_Success(t *testing.T) {
	content := []byte("test content")
	tmpfile, err := os.CreateTemp("", "example")
	assert.Nil(t, err)
	defer os.Remove(tmpfile.Name())

	_, err = tmpfile.Write(content)
	assert.Nil(t, err)
	assert.Nil(t, tmpfile.Close())

	data, err := FileReadAll(tmpfile.Name())

	assert.Nil(t, err)
	assert.Equal(t, content, data)
}

func TestFileReadAll_FileNotExist(t *testing.T) {
	data, err := FileReadAll("nonexistent_file")

	assert.NotNil(t, err)
	assert.Nil(t, data)
	assert.True(t, os.IsNotExist(err))
}

func TestFileReadAll_OpenError(t *testing.T) {
	// create a directory which can't be opened as a file
	tmpDir, err := os.MkdirTemp("", "testdir")
	assert.Nil(t, err)
	defer os.RemoveAll(tmpDir)

	data, err := FileReadAll(tmpDir)

	assert.NotNil(t, err)
	assert.Nil(t, data)
}

func TestEnsureDir_Success(t *testing.T) {
	dir := "tmpdir"

	err := os.Mkdir(dir, 0755)
	defer os.Remove(dir)
	assert.Nil(t, err)

	err = ensureDir(dir)
	assert.Nil(t, err)
}

func TestEnsureDir_CreateIfNotExist(t *testing.T) {
	dir := "tmpdir"

	err := ensureDir(dir)
	defer os.Remove(dir)

	assert.Nil(t, err)
	info, err := os.Stat(dir)
	assert.Nil(t, err)
	assert.True(t, info.IsDir())
}

func TestEnsureDir_Fail(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "testfile")
	assert.Nil(t, err)
	defer os.Remove(tmpFile.Name())

	err = ensureDir(tmpFile.Name())
	assert.NotNil(t, err)
}

func TestWriteToFile_FileExist(t *testing.T) {
	var (
		existFileName string
		oldContent    = []byte("old content")
		newContent    = []byte("new content")
	)
	// prepare a exist file with data
	existFile, _ := os.CreateTemp("", "TestWriteToFile_FileExist")
	existFileName = existFile.Name()

	existFile.Write(oldContent)
	existFile.Close()

	err := WriteToFile(existFileName, newContent)
	assert.Nil(t, err)

	check := func(t *testing.T, path string, expectedData []byte) {
		f, err := os.Open(path)
		defer os.Remove(path)

		assert.Nil(t, err)
		data, err := io.ReadAll(f)
		assert.Nil(t, err)
		assert.Equal(t, expectedData, data)
	}

	check(t, existFileName, newContent)
	check(t, existFileName+".bak", oldContent)
}
