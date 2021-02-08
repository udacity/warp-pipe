package integration

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx"
	warppipe "github.com/perangel/warp-pipe"
	"github.com/perangel/warp-pipe/db"
	"github.com/stretchr/testify/require"
)

type TestData struct {
	text    string
	date    time.Time
	boolean bool
	json    string
	jsonb   string
	array   []int32
}

var (
	dbUser     = "test"
	dbPassword = "test"
	dbName     = "test"
	testSchema = []string{
		`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`,
		`CREATE TABLE "testTable" (
			ID UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
			type_text TEXT,
			type_date DATE,
			type_boolean BOOLEAN,
			type_json JSON,
			type_jsonb JSONB,
			type_ARRAY int4[]
		)`,
	}
)

func setupTestSchema(config pgx.ConnConfig) error {
	//wait until db is ready to obtain connection
	srcReady := waitForPostgresReady(&config)
	if !srcReady {
		return fmt.Errorf("database did not become ready in allowed time")
	}

	conn, err := pgx.Connect(config)
	if err != nil {
		return err
	}
	defer conn.Close()

	for _, s := range testSchema {
		_, err = conn.Exec(s)
		if err != nil {
			return fmt.Errorf("Test schema installation failed: %v", err)
		}
	}
	return nil
}

func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func waitForPostgresReady(config *pgx.ConnConfig) bool {
	connected := false
	for count := 0; count < 30; count++ {
		conn, err := pgx.Connect(*config)
		if err == nil {
			defer conn.Close()
			connected = true
			break
		}
		time.Sleep(2 * time.Second)
	}
	return connected
}

func createDatabaseContainer(t *testing.T, ctx context.Context, version string, database string, username string, password string) (string, int, error) {
	docker, err := NewDockerClient()
	if err != nil {
		return "", 0, err
	}
	postgresPort := 5432
	hostPort, err := getFreePort()
	if err != nil {
		return "", 0, errors.New("could not determine a free port")
	}
	container, err := docker.runContainer(
		ctx,
		&ContainerConfig{
			image: fmt.Sprintf("postgres:%s", version),
			ports: []*PortMapping{
				&PortMapping{
					HostPort:      fmt.Sprintf("%d", hostPort),
					ContainerPort: fmt.Sprintf("%d", postgresPort),
				},
			},
			env: []string{
				fmt.Sprintf("POSTGRES_DB=%s", database),
				fmt.Sprintf("POSTGRES_USER=%s", username),
				fmt.Sprintf("POSTGRES_PASSWORD=%s", password),
			},
			cmd: []string{
				"postgres",
				"-cwal_level=logical",
				"-cmax_replication_slots=1",
				"-cmax_wal_senders=1",
			},
		})
	if err != nil {
		return "", 0, err
	}

	t.Cleanup(func() {
		if err := docker.removeContainer(ctx, container.ID); err != nil {
			t.Errorf("Could not remove container %s: %w", container.ID, err)
		}
	})

	return container.ID, hostPort, nil
}

func testRow() *TestData {
	row := TestData{}
	row.text = "a test string"
	row.boolean = false
	row.date = time.Now()
	row.json = `{"name": "Alice", "age": 31, "city": "LA"}`
	row.jsonb = `{"name": "Bob", "age": 39, "city": "London"}`
	row.array = []int32{1, 2, 3, 4, 5}
	return &row
}

func insertTestData(t *testing.T, config pgx.ConnConfig, wg *sync.WaitGroup) {
	defer wg.Done()
	nRows := 50
	conn, err := pgx.Connect(config)
	if err != nil {
		t.Logf("%s: could not connected to source database to insert: %v", t.Name(), err)
		return
	}
	defer conn.Close()

	insertSQL := `INSERT INTO
	"testTable"(type_text, type_date, type_boolean, type_json, type_jsonb, type_ARRAY)
	VALUES ($1, $2, $3, $4, $5, $6);`
	for i := 0; i < nRows; i++ {
		row := testRow()
		_, err = conn.Exec(insertSQL, row.text, row.date, row.boolean, row.json, row.jsonb, row.array)
		if err != nil {
			t.Logf("%s: Could not insert row in source database: %v", t.Name(), err)
		}
	}
}

func updateTestData(t *testing.T, config pgx.ConnConfig, wg *sync.WaitGroup) {
	defer wg.Done()
	conn, err := pgx.Connect(config)
	if err != nil {
		t.Logf("%s: could not connected to source database to update: %v", t.Name(), err)
		return
	}
	defer conn.Close()

	//update one field in one row
	updateSQL := `UPDATE "testTable" set type_boolean = true WHERE ID IN (SELECT ID FROM "testTable" where type_boolean = false LIMIT 1);`
	_, err = conn.Exec(updateSQL)
	if err != nil {
		t.Logf("%s: Could not update row in source database: %v", t.Name(), err)
	}
}

func deleteTestData(t *testing.T, config pgx.ConnConfig, wg *sync.WaitGroup) {
	defer wg.Done()
	conn, err := pgx.Connect(config)
	if err != nil {
		t.Logf("%s: Could not connect to source db to delete data", t.Name())
		return
	}
	defer conn.Close()

	// delete a row
	deleteSQL := `DELETE FROM "testTable" WHERE ID IN (SELECT ID FROM "testTable" LIMIT 1);`
	_, err = conn.Exec(deleteSQL)
	if err != nil {
		t.Logf("%s: Could not delete row in source database: %v", t.Name(), err)
	}
}

func verify(t *testing.T, src, target pgx.ConnConfig) (bool, error) {
	dataMatches := true
	sourceConn, err := pgx.Connect(src)
	if err != nil {
		return false, fmt.Errorf("could not connect to source database: %v", err)
	}
	defer sourceConn.Close()

	targetConn, err := pgx.Connect(target)
	if err != nil {
		return false, fmt.Errorf("could not connect to source database: %v", err)
	}
	defer targetConn.Close()

	verificationQueries := []string{
		`select count(*) from "testTable";`,
		`select count(*) from "testTable" where type_boolean = true;`,
	}

	for _, q := range verificationQueries {
		var srcCount, targetCount int
		res, err := sourceConn.Query(q)
		if err != nil {
			return false, fmt.Errorf("could not run query against source: %v", err)
		}
		for res.Next() {
			err := res.Scan(&srcCount)
			if err != nil {
				return false, fmt.Errorf("could not query result from source: %v", err)
			}
		}
		res.Close()
		res, err = targetConn.Query(q)
		if err != nil {
			return false, fmt.Errorf("could not run query against target: %v", err)
		}
		for res.Next() {
			err := res.Scan(&targetCount)
			if err != nil {
				return false, fmt.Errorf("could not query result from source: %v", err)
			}
		}
		res.Close()
		if srcCount != targetCount {
			t.Logf("results do not match: query: %s, sourceCount: %d, targetCount: %d, \n", q, srcCount, targetCount)
			dataMatches = false
		}
	}
	return dataMatches, nil
}

func TestVersionMigration(t *testing.T) {
	testCases := []struct {
		name   string
		source string
		target string
	}{
		{
			name:   "9.5To9.6",
			source: "9.5",
			target: "9.6",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			//bring up source and target database containers
			_, srcPort, err := createDatabaseContainer(t, ctx, tc.source, dbUser, dbPassword, dbName)
			require.NoError(t, err)
			srcDBConfig := pgx.ConnConfig{
				Host:     "127.0.0.1",
				Port:     uint16(srcPort),
				User:     dbUser,
				Password: dbPassword,
				Database: dbName,
			}
			err = setupTestSchema(srcDBConfig)
			require.NoError(t, err)

			_, targetPort, err := createDatabaseContainer(t, ctx, tc.target, dbUser, dbPassword, dbName)
			require.NoError(t, err)
			targetDBConfig := pgx.ConnConfig{
				Host:     "127.0.0.1",
				Port:     uint16(targetPort),
				User:     dbUser,
				Password: dbPassword,
				Database: dbName,
			}
			err = setupTestSchema(targetDBConfig)
			require.NoError(t, err)

			// setup warp-pipe on source database
			wpConn, err := pgx.Connect(srcDBConfig)
			require.NoError(t, err)
			err = db.Prepare(wpConn, []string{"public"}, []string{"testTable"}, []string{})
			if err != nil {
				t.Errorf("Could not setup warp pipe: %v", err)
			}

			// write, update, delete to produce change sets
			var insertsWG, updatesWG, deletesWG sync.WaitGroup
			workersCount := 20
			for i := 0; i < workersCount; i++ {
				insertsWG.Add(1)
				go insertTestData(t, srcDBConfig, &insertsWG)
			}

			for i := 0; i < workersCount; i++ {
				updatesWG.Add(1)
				go updateTestData(t, srcDBConfig, &updatesWG)
			}

			for i := 0; i < workersCount; i++ {
				deletesWG.Add(1)
				go deleteTestData(t, srcDBConfig, &deletesWG)
			}

			// sync source and target with Axon
			axonCfg := warppipe.AxonConfig{
				SourceDBHost:   srcDBConfig.Host,
				SourceDBPort:   srcPort,
				SourceDBName:   srcDBConfig.Database,
				SourceDBUser:   srcDBConfig.User,
				SourceDBPass:   srcDBConfig.Password,
				TargetDBHost:   targetDBConfig.Host,
				TargetDBPort:   targetPort,
				TargetDBName:   targetDBConfig.Database,
				TargetDBUser:   targetDBConfig.User,
				TargetDBPass:   targetDBConfig.Password,
				TargetDBSchema: "public",
			}
			axon := warppipe.Axon{Config: &axonCfg}
			axon.Run()

			// wait for all our routines to complete
			insertsWG.Wait()
			updatesWG.Wait()
			deletesWG.Wait()

			// sync one more time to catch any stragglers
			axon.Run()

			// run simple count queries to verify if the data was synced correctly.
			// TODO: replace with checksum approach
			dataMatches, err := verify(t, srcDBConfig, targetDBConfig)
			require.NoError(t, err)
			if !dataMatches {
				t.Error("Data integrity checks failed")
			}
		})
	}
}
