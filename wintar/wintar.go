package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"compress/gzip"

	"github.com/Microsoft/go-winio"
	"github.com/Microsoft/go-winio/archive/tar"
	"github.com/Microsoft/go-winio/backuptar"
)
import "flag"

func main() {
	err := run()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func run() error {
	var (
		backup     bool
		sd         bool
		root       string
		outputFile string
		doGzip     bool
	)

	flag.BoolVar(&backup, "backup", false, "Take backup privilege when accessing files")
	flag.BoolVar(&sd, "security", false, "Include security descriptors")
	flag.StringVar(&root, "root", "", "Directory to archive")
	flag.StringVar(&outputFile, "out", "-", "Output file")
	flag.BoolVar(&doGzip, "gzip", false, "Compress with gzip")

	flag.Parse()

	var output io.Writer
	if outputFile == "-" {
		output = os.Stdout
	} else {
		f, err := os.Create(outputFile)
		if err != nil {
			return err
		}
		defer f.Close()
		output = f
	}

	if doGzip {
		gw := gzip.NewWriter(output)
		defer gw.Close()
		output = gw
	}

	fn := createArchive
	if backup {
		fn = func(root string, t *tar.Writer, sd bool) error {
			return winio.RunWithPrivilege("SeBackupPrivilege", func() error { return createArchive(root, t, sd) })
		}
	}

	bw := bufio.NewWriter(output)
	t := tar.NewWriter(bw)

	err := fn(root, t, sd)
	if err == nil {
		err = t.Close()
		if err == nil {
			err = bw.Flush()
		}
	}

	if err != nil {
		return err
	}
	return nil
}

func createArchive(root string, t *tar.Writer, sd bool) error {
	fileIDs := make(map[winio.FileIDInfo]string)
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		f, err := winio.OpenForBackup(path, syscall.GENERIC_READ, syscall.FILE_SHARE_READ, syscall.OPEN_EXISTING)
		if err != nil {
			return err
		}
		defer f.Close()

		bi, err := winio.GetFileBasicInfo(f)
		if err != nil {
			return err
		}

		fileID, err := winio.GetFileID(f)
		if err != nil {
			return err
		}

		name, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}

		if link, ok := fileIDs[*fileID]; ok {
			hdr := backuptar.BasicInfoHeader(name, 0, bi)
			hdr.Typeflag = tar.TypeLink
			hdr.Linkname = filepath.ToSlash(link)
			err = t.WriteHeader(hdr)
			if err != nil {
				return err
			}
			return nil
		}

		fileIDs[*fileID] = name

		r := winio.NewBackupFileReader(f, sd)
		defer r.Close()
		br := bufio.NewReader(r)
		err = backuptar.WriteTarFileFromBackupStream(t, br, name, info.Size(), bi)
		if err != nil {
			return err
		}
		return nil
	})
	return err
}
