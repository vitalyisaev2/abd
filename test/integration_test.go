package test

import (
	"context"
	"fmt"
	"sync"
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

	t.Run("concurrent writes and reads to the same nodes", func(t *testing.T) {
		// cluster from 3 processes
		cluster, err := newLocalhostCluster(3)
		require.NoError(t, err)

		defer cluster.quit()

		// pick different nodes for reading and for writing
		processIDs := cluster.getProcessIDs()

		var (
			wg            sync.WaitGroup
			writtenValues []utils.Value
			readValues    []utils.Value
		)

		const iterations = 100

		wg.Add(2)

		// one thread is for reading
		go func() {
			defer wg.Done()

			// preserve node for reading
			readProcess, err := cluster.getProcessByID(processIDs[0])
			require.NoError(t, err)

			ctx := context.Background()

			for i := 0; i < iterations; i++ {
				value, err := readProcess.Read(ctx)
				require.NoError(t, err)
				readValues = append(readValues, value)
			}
		}()

		// another is for writing
		go func() {
			defer wg.Done()

			// preserve node for writing
			writerProcess, err := cluster.getProcessByID(processIDs[len(processIDs)-1])
			require.NoError(t, err)

			ctx := context.Background()

			for i := 0; i < iterations; i++ {
				value := utils.Value(i)
				err := writerProcess.Write(ctx, value)
				require.NoError(t, err)

				writtenValues = append(writtenValues, value)
			}
		}()

		wg.Wait()

		// TODO: linearizability checks
		fmt.Println(writtenValues)
		fmt.Println(readValues)
	})

	t.Run("concurrent writes and reads to different nodes", func(t *testing.T) {
		// cluster from 3 processes
		cluster, err := newLocalhostCluster(3)
		require.NoError(t, err)

		defer cluster.quit()

		var (
			wg            sync.WaitGroup
			writtenValues []utils.Value
			readValues    []utils.Value
		)

		const iterations = 100

		wg.Add(2)

		// one thread is for reading
		go func() {
			defer wg.Done()

			ctx := context.Background()

			for i := 0; i < iterations; i++ {
				// get random node every time
				value, err := cluster.getRandomProcess().Read(ctx)
				require.NoError(t, err)
				readValues = append(readValues, value)
			}
		}()

		// another is for writing
		go func() {
			defer wg.Done()

			ctx := context.Background()

			for i := 0; i < iterations; i++ {
				// get random node every time
				value := utils.Value(i)
				err := cluster.getRandomProcess().Write(ctx, value)
				require.NoError(t, err)

				writtenValues = append(writtenValues, value)
			}
		}()

		wg.Wait()

		// TODO: linearizability checks
		fmt.Println(writtenValues)
		fmt.Println(readValues)
	})
}
