//go:build nos3
// +build nos3

package vfs

import (
	"errors"

	"github.com/drakkan/sftpgo/v2/version"
)

func init() {
	version.AddFeature("-Manta")
}

// NewMantaFs returns an error, Manta is disabled
func NewMantaFs(connectionID, localTempDir, mountPath string, config MantaFsConfig) (Fs, error) {
	return nil, errors.New("Manta disabled at build time")
}
