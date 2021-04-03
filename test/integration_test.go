package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestABD(t *testing.T) {
	t.Run("simple read write", func(t *testing.T) {
		// cluster from 3 processes
		cluster, err := newLocalhostCluster(3)
		require.NoError(t, err)

		defer cluster.quit()

		process := cluster.getRandomProcess()

		// initial value is zero
		value, err := process.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, 0, value)

		// write new value
		err = process.Write(context.Background(), 1)
		require.NoError(t, err)
		require.Equal(t, 1, value)

		// read written value
		value, err = process.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, 1, value)
	})
}
