// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package parquetlogencodingextension // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/parquetlogencodingextension"

import (
	"io"
	"path/filepath"

	"github.com/spf13/afero"
	"github.com/xitongsys/parquet-go/source"
)

type OnCloseFunc func(string, io.Reader) error

type MemFile struct {
	FilePath string
	File     afero.File
	Fs       afero.Fs
	OnClose  OnCloseFunc
}

func NewMemFileWriter(name string, f OnCloseFunc) (source.ParquetFile, error) {
	m := MemFile{
		Fs:      afero.NewMemMapFs(),
		OnClose: f,
	}
	return m.Create(name)
}

func (fs *MemFile) Create(name string) (source.ParquetFile, error) {
	file, err := fs.Fs.Create(name)
	if err != nil {
		return fs, err
	}

	fs.File = file
	fs.FilePath = name
	return fs, nil
}

func (fs *MemFile) Open(name string) (source.ParquetFile, error) {
	if name == "" {
		name = fs.FilePath
	}

	file, err := fs.Fs.Open(name)
	if err != nil {
		return fs, err
	}
	fs.File = file
	fs.FilePath = name
	return fs, nil
}

func (fs *MemFile) Seek(offset int64, pos int) (int64, error) {
	return fs.File.Seek(offset, pos)
}

func (fs *MemFile) Read(b []byte) (int, error) {
	var (
		total int
		err   error
	)
	for total < len(b) {
		var n int
		n, err = fs.File.Read(b[total:])
		total += n
		if err != nil {
			break
		}
	}
	return total, err
}

func (fs *MemFile) Write(b []byte) (int, error) {
	return fs.File.Write(b)
}

func (fs *MemFile) Close() error {
	if err := fs.File.Close(); err != nil {
		return err
	}
	if fs.OnClose != nil {
		f, err := fs.Open(fs.FilePath)
		if err != nil {
			return err
		}
		defer f.Close()
		if err := fs.OnClose(filepath.Base(fs.FilePath), f); err != nil {
			return err
		}
	}
	return nil
}
