package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/vitalyisaev2/abd/utils"
)

func TestABD(t *testing.T) {
	t.Run("read write on the same process", func(t *testing.T) {
		// cluster from 3 processes
		cluster, err := newLocalhostCluster(3)
		require.NoError(t, err)

		defer cluster.quit()

		process := cluster.getRandomProcess()

		// initial value is zero
		value, err := process.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, utils.Value(0), value)

		// write new value
		err = process.Write(context.Background(), 1)
		require.NoError(t, err)

		// read written value
		value, err = process.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, utils.Value(1), value)
	})

	t.Run("read write on the different processes", func(t *testing.T) {
		// cluster from 3 processes
		cluster, err := newLocalhostCluster(3)
		require.NoError(t, err)

		defer cluster.quit()

		// pick different nodes for reading and for writing
		processIDs := cluster.getProcessIDs()
		readProcess, err := cluster.getProcessByID(processIDs[0])
		require.NoError(t, err)
		writerProcess, err := cluster.getProcessByID(processIDs[len(processIDs)-1])
		require.NoError(t, err)

		// initial value is zero
		value, err := readProcess.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, utils.Value(0), value)

		// write new value
		err = writerProcess.Write(context.Background(), -123)
		require.NoError(t, err)

		// read written value
		value, err = readProcess.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, utils.Value(-123), value)
	})
}
