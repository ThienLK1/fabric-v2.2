/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package statemongodb

// import (
// 	"testing"

// 	"github.com/hyperledger/fabric/common/metrics/disabled"
// 	"github.com/hyperledger/fabric/core/ledger"
// 	"github.com/hyperledger/fabric/integration/runner"
// 	"github.com/stretchr/testify/require"
// )

// // StartMongoDB starts the MongoDB if it is not running already
// func StartMongoDB(t *testing.T, binds []string) (addr string, stopMongoDBFunc func()) {
// 	mongoDB := &runner.MongoDB{Binds: binds}
// 	require.NoError(t, mongoDB.Start())
// 	return mongoDB.Address(), func() { mongoDB.Stop() }
// }

// // IsEmpty returns whether or not the mongodb is empty
// func IsEmpty(t testing.TB, config *ledger.MongoDBConfig) bool {
// 	mongoInstance, err := createMongoInstance(config, &disabled.Provider{})
// 	require.NoError(t, err)
// 	dbEmpty, err := mongoInstance.isEmpty(nil)
// 	require.NoError(t, err)
// 	return dbEmpty
// }
