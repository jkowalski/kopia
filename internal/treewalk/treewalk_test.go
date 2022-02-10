package treewalk_test

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/internal/treewalk"
	"github.com/kopia/kopia/repo/logging"
	"github.com/stretchr/testify/require"
)

func Test(t *testing.T) {
	var log = logging.Module("test")

	var cnt int32

	ctx := testlogging.Context(t)

	require.NoError(t, treewalk.InParallel(ctx, 1, func(ctx context.Context, it interface{}, processChild treewalk.ReportChildFunc) error {
		dir := it.(string)

		log(ctx).Infof("dir: %v %v", atomic.AddInt32(&cnt, 1), dir)

		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}

		for _, ent := range entries {
			if ent.IsDir() {
				if err := processChild(ctx, filepath.Join(dir, ent.Name())); err != nil {
					return err
				}
			}
		}

		return nil
	}, []interface{}{"..\\.."}))

	t.Error()
}
