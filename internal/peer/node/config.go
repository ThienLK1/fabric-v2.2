/*
Copyright IBM Corp. 2016 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package node

import (
	"github.com/hyperledger/fabric/common/flogging"
	coreconfig "github.com/hyperledger/fabric/core/config"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/spf13/viper"
	"path/filepath"
	"regexp"
	"strings"
)

const (
	couchDB = "couchdb"
	mongoDB = "mongodb"
)

var configLogger = flogging.MustGetLogger("configLogger")

// escapeUpperCase replaces every upper case letter with a '$' and the respective
// lower-case letter
func escapeUpperCase(stateDatabaseName string) string {
	re := regexp.MustCompile(`([A-Z])`)
	stateDatabaseName = re.ReplaceAllString(stateDatabaseName, "$$"+"$1")
	return strings.ToLower(stateDatabaseName)
}

func ledgerConfig() *ledger.Config {
	// set defaults
	warmAfterNBlocks := 1
	stateDB := escapeUpperCase(viper.GetString("ledger.state.stateDatabase"))
	internalQueryLimit := 1000
	maxBatchUpdateSize := 500
	collElgProcMaxDbBatchSize := 5000
	collElgProcDbBatchesInterval := 1000
	if couchDB == stateDB {
		if viper.IsSet("ledger.state.couchDBConfig.warmIndexesAfterNBlocks") {
			warmAfterNBlocks = viper.GetInt("ledger.state.couchDBConfig.warmIndexesAfterNBlocks")
		}
		if viper.IsSet("ledger.state.couchDBConfig.internalQueryLimit") {
			internalQueryLimit = viper.GetInt("ledger.state.couchDBConfig.internalQueryLimit")
		}
		if viper.IsSet("ledger.state.couchDBConfig.maxBatchUpdateSize") {
			maxBatchUpdateSize = viper.GetInt("ledger.state.couchDBConfig.maxBatchUpdateSize")
		}
	} else if mongoDB == stateDB {
		if viper.IsSet("ledger.state.mongoDBConfig.warmIndexesAfterNBlocks") {
			warmAfterNBlocks = viper.GetInt("ledger.state.mongoDBConfig.warmIndexesAfterNBlocks")
		}
		if viper.IsSet("ledger.state.mongoDBConfig.queryLimit") {
			internalQueryLimit = viper.GetInt("ledger.state.mongoDBConfig.queryLimit")
		}
		if viper.IsSet("ledger.state.mongoDBConfig.maxBatchUpdateSize") {
			maxBatchUpdateSize = viper.GetInt("ledger.state.mongoDBConfig.maxBatchUpdateSize")
		}
	}
	purgeInterval := 100
	if viper.IsSet("ledger.pvtdataStore.purgeInterval") {
		purgeInterval = viper.GetInt("ledger.pvtdataStore.purgeInterval")
	}

	rootFSPath := filepath.Join(coreconfig.GetPath("peer.fileSystemPath"), "ledgersData")
	snapshotsRootDir := viper.GetString("ledger.snapshots.rootDir")
	if snapshotsRootDir == "" {
		snapshotsRootDir = filepath.Join(rootFSPath, "snapshots")
	}
	conf := &ledger.Config{
		RootFSPath: rootFSPath,
		StateDBConfig: &ledger.StateDBConfig{
			StateDatabase: viper.GetString("ledger.state.stateDatabase"),
			CouchDB:       &ledger.CouchDBConfig{},
			MongoDB:       &ledger.MongoDBConfig{},
		},
		PrivateDataConfig: &ledger.PrivateDataConfig{
			MaxBatchSize:    collElgProcMaxDbBatchSize,
			BatchesInterval: collElgProcDbBatchesInterval,
			PurgeInterval:   purgeInterval,
		},
		HistoryDBConfig: &ledger.HistoryDBConfig{
			Enabled: viper.GetBool("ledger.history.enableHistoryDatabase"),
		},
		SnapshotsConfig: &ledger.SnapshotsConfig{
			RootDir: snapshotsRootDir,
		},
	}

	if conf.StateDBConfig.StateDatabase == couchDB {
		conf.StateDBConfig.CouchDB = &ledger.CouchDBConfig{
			Address:                 viper.GetString("ledger.state.couchDBConfig.couchDBAddress"),
			Username:                viper.GetString("ledger.state.couchDBConfig.username"),
			Password:                viper.GetString("ledger.state.couchDBConfig.password"),
			MaxRetries:              viper.GetInt("ledger.state.couchDBConfig.maxRetries"),
			MaxRetriesOnStartup:     viper.GetInt("ledger.state.couchDBConfig.maxRetriesOnStartup"),
			RequestTimeout:          viper.GetDuration("ledger.state.couchDBConfig.requestTimeout"),
			InternalQueryLimit:      internalQueryLimit,
			MaxBatchUpdateSize:      maxBatchUpdateSize,
			WarmIndexesAfterNBlocks: warmAfterNBlocks,
			CreateGlobalChangesDB:   viper.GetBool("ledger.state.couchDBConfig.createGlobalChangesDB"),
			RedoLogPath:             filepath.Join(rootFSPath, "couchdbRedoLogs"),
			UserCacheSizeMBs:        viper.GetInt("ledger.state.couchDBConfig.cacheSize"),
		}
	} else if conf.StateDBConfig.StateDatabase == mongoDB {
		conf.StateDBConfig.MongoDB = &ledger.MongoDBConfig{
			Address:      viper.GetString("ledger.state.mongoDBConfig.mongoDBAddress"),
			Username:     viper.GetString("ledger.state.mongoDBConfig.username"),
			DatabaseName: "statemongodb",
			Password:     viper.GetString("ledger.state.mongoDBConfig.password"),
			//MaxRetries:              viper.GetInt("ledger.state.mongoDBConfig.maxRetries"),
			//MaxRetriesOnStartup:     viper.GetInt("ledger.state.mongoDBConfig.maxRetriesOnStartup"),
			//RequestTimeout:          viper.GetDuration("ledger.state.mongoDBConfig.requestTimeout"),
			MaxRetries:              3,
			MaxRetriesOnStartup:     3,
			RequestTimeout:          35000000000,
			QueryLimit:              internalQueryLimit,
			MaxBatchUpdateSize:      maxBatchUpdateSize,
			WarmIndexesAfterNBlocks: warmAfterNBlocks,
			RedoLogPath:             filepath.Join(rootFSPath, "mongoRedoLogs"),
			UserCacheSizeMBs:        viper.GetInt("ledger.state.mongoDBConfig.cacheSize"),
		}
		configLogger.Debugf("conf.StateDBConfig.MongoDB %v", conf.StateDBConfig.MongoDB)
	}
	return conf
}
