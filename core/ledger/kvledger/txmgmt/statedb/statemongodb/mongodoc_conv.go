/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package statemongodb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/hyperledger/fabric/core/ledger/internal/version"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/pkg/errors"
)

const (
	binaryField   = "_binaryData"
	binaryWrapper = "valueBytes"
	idField       = "_id"
	revField      = "_rev"
	versionField  = "~version"
	deletedField  = "_deleted"
)

type keyValue struct {
	key      string
	revision string
	*statedb.VersionedValue
}

type jsonValue map[string]interface{}

func tryCastingToJSON(b []byte) (isJSON bool, val jsonValue) {
	var jsonVal map[string]interface{}
	err := json.Unmarshal(b, &jsonVal)
	return err == nil, jsonValue(jsonVal)
}

func castToJSON(b []byte) (jsonValue, error) {
	var jsonVal map[string]interface{}
	err := json.Unmarshal(b, &jsonVal)
	err = errors.Wrap(err, "error unmarshalling json data")
	return jsonVal, err
}

func (v jsonValue) checkReservedFieldsNotPresent() error {
	for fieldName := range v {
		if fieldName == versionField || strings.HasPrefix(fieldName, "_") {
			return errors.Errorf("field [%s] is not valid for the MongoDB state database", fieldName)
		}
	}
	return nil
}

func (v jsonValue) removeRevField() {
	delete(v, revField)
}

func (v jsonValue) toBytes() ([]byte, error) {
	jsonBytes, err := json.Marshal(v)
	err = errors.Wrap(err, "error marshalling json data")
	return jsonBytes, err
}

func mongoDocToKeyValue(doc *mongoDoc) (*keyValue, error) {
	docFields, err := validateAndRetrieveFields(doc)
	if err != nil {
		return nil, err
	}
	version, metadata, err := decodeVersionAndMetadata(docFields.versionAndMetadata)
	if err != nil {
		return nil, err
	}
	return &keyValue{
		docFields.id, docFields.revision,
		&statedb.VersionedValue{
			Value:    docFields.value,
			Version:  version,
			Metadata: metadata,
		},
	}, nil
}

type mongoDocFields struct {
	id                 string
	revision           string
	value              []byte
	versionAndMetadata string
}

func validateAndRetrieveFields(doc *mongoDoc) (*mongoDocFields, error) {
	jsonDoc := make(jsonValue)
	decoder := json.NewDecoder(bytes.NewBuffer(doc.jsonValue))
	decoder.UseNumber()
	if err := decoder.Decode(&jsonDoc); err != nil {
		return nil, err
	}
	docFields := &mongoDocFields{}
	docFields.id = jsonDoc[idField].(string)
	if jsonDoc[revField] != nil {
		docFields.revision = string(jsonDoc[revField].(json.Number))
	}
	if jsonDoc[versionField] == nil {
		return nil, fmt.Errorf("version field %s was not found", versionField)
	}
	docFields.versionAndMetadata = jsonDoc[versionField].(string)
	delete(jsonDoc, idField)
	delete(jsonDoc, revField)
	delete(jsonDoc, versionField)

	var err error
	if doc.binaryDatas == nil {
		docFields.value, err = json.Marshal(jsonDoc)
		return docFields, err
	}
	for _, binaryData := range doc.binaryDatas {
		if binaryData.Name == binaryWrapper {
			docFields.value = binaryData.Binarydata
		}
	}
	// handle binary or json data
	if doc.binaryDatas != nil { // binary attachment
		// get binary data from attachment
		for _, attachment := range doc.binaryDatas {
			if attachment.Name == binaryField {
				docFields.value = attachment.Binarydata
			}
		}
	} else {
		// marshal the returned JSON data.
		if docFields.value, err = json.Marshal(jsonDoc); err != nil {
			return nil, err
		}
	}
	logger.Debugf("validateAndRetrieveFields docFields after : %+v", docFields)
	return docFields, err
}

func keyValToMongoDoc(kv *keyValue) (*mongoDoc, error) {
	type kvType int32
	const (
		kvTypeDelete = iota
		kvTypeJSON
		kvTypeBinaryData
	)
	key, value, metadata, version := kv.key, kv.Value, kv.Metadata, kv.Version

	jsonMap := make(jsonValue)

	var kvtype kvType
	switch {
	case value == nil:
		kvtype = kvTypeDelete
	// check for the case where the jsonMap is nil,  this will indicate
	// a special case for the Unmarshal that results in a valid JSON returning nil
	case json.Unmarshal(value, &jsonMap) == nil && jsonMap != nil:
		kvtype = kvTypeJSON
		if err := jsonMap.checkReservedFieldsNotPresent(); err != nil {
			return nil, err
		}
	default:
		// create an empty map, if the map is nil
		if jsonMap == nil {
			jsonMap = make(jsonValue)
		}
		kvtype = kvTypeBinaryData
	}

	verAndMetadata, err := encodeVersionAndMetadata(version, metadata)
	if err != nil {
		return nil, err
	}
	// add the (version + metadata), id, revision, and delete marker (if needed)
	jsonMap[versionField] = verAndMetadata
	jsonMap[idField] = key
	if kv.revision != "" {
		jsonMap[revField] = kv.revision
	}
	if kvtype == kvTypeDelete {
		jsonMap[deletedField] = true
	}

	//jsonMap[valueField] = value
	jsonBytes, err := jsonMap.toBytes()
	if err != nil {
		return nil, err
	}
	mongoDoc := &mongoDoc{jsonValue: jsonBytes}

	if kvtype == kvTypeBinaryData {
		binaryData := &BinaryDataInfo{}
		binaryData.Binarydata = value
		binaryData.Name = binaryField
		binaryDatas := append([]*BinaryDataInfo{}, binaryData)
		mongoDoc.binaryDatas = binaryDatas
	}
	return mongoDoc, nil
}

// mongoSavepointData data for mongodb
type mongoSavepointData struct {
	BlockNum uint64 `json:"BlockNum"`
	TxNum    uint64 `json:"TxNum"`
}

type channelMetadata struct {
	ChannelName string `json:"ChannelName"`
	// namespace to namespaceDBInfo mapping
	NamespaceDBsInfo map[string]*namespaceDBInfo `json:"NamespaceDBsInfo"`
}

type namespaceDBInfo struct {
	Namespace string `json:"Namespace"`
	DBName    string `json:"DBName"`
}

func encodeSavepoint(height *version.Height) (*mongoDoc, error) {
	var err error
	var savepointDoc mongoSavepointData
	// construct savepoint document
	savepointDoc.BlockNum = height.BlockNum
	savepointDoc.TxNum = height.TxNum
	savepointDocJSON, err := json.Marshal(savepointDoc)
	if err != nil {
		err = errors.Wrap(err, "failed to marshal savepoint data")
		logger.Errorf("%+v", err)
		return nil, err
	}
	return &mongoDoc{jsonValue: savepointDocJSON}, nil
}

func decodeSavepoint(mongoDoc *mongoDoc) (*version.Height, error) {
	savepointDoc := &mongoSavepointData{}
	if err := json.Unmarshal(mongoDoc.jsonValue, &savepointDoc); err != nil {
		err = errors.Wrap(err, "failed to unmarshal savepoint data")
		logger.Errorf("%+v", err)
		return nil, err
	}
	return &version.Height{BlockNum: savepointDoc.BlockNum, TxNum: savepointDoc.TxNum}, nil
}

func encodeChannelMetadata(metadataDoc *channelMetadata) (*mongoDoc, error) {
	metadataJSON, err := json.Marshal(metadataDoc)
	if err != nil {
		err = errors.Wrap(err, "failed to marshal channel metadata")
		logger.Errorf("%+v", err)
		return nil, err
	}
	return &mongoDoc{jsonValue: metadataJSON, binaryDatas: nil}, nil
}

func decodeChannelMetadata(mongoDoc *mongoDoc) (*channelMetadata, error) {
	metadataDoc := &channelMetadata{}
	if err := json.Unmarshal(mongoDoc.jsonValue, &metadataDoc); err != nil {
		err = errors.Wrap(err, "failed to unmarshal channel metadata")
		logger.Errorf("%+v", err)
		return nil, err
	}
	return metadataDoc, nil
}

type dataformatInfo struct {
	Version string `json:"Version"`
}

func encodeDataformatInfo(dataFormatVersion string) (*mongoDoc, error) {
	var err error
	dataformatInfo := &dataformatInfo{
		Version: dataFormatVersion,
	}
	dataformatInfoJSON, err := json.Marshal(dataformatInfo)
	if err != nil {
		err = errors.Wrapf(err, "failed to marshal dataformatInfo [%#v]", dataformatInfo)
		logger.Errorf("%+v", err)
		return nil, err
	}
	return &mongoDoc{jsonValue: dataformatInfoJSON, binaryDatas: nil}, nil
}

func decodeDataformatInfo(mongoDoc *mongoDoc) (string, error) {
	dataformatInfo := &dataformatInfo{}
	if err := json.Unmarshal(mongoDoc.jsonValue, dataformatInfo); err != nil {
		err = errors.Wrapf(err, "failed to unmarshal json [%#v] into dataformatInfo", mongoDoc.jsonValue)
		logger.Errorf("%+v", err)
		return "", err
	}
	version := regexp.MustCompile(`^"(.*)"$`).ReplaceAllString(dataformatInfo.Version, `$1`)
	return version, nil
}

func validateValue(value []byte) error {
	isJSON, jsonVal := tryCastingToJSON(value)
	if !isJSON {
		return nil
	}
	return jsonVal.checkReservedFieldsNotPresent()
}

func validateKey(key string) error {
	if !utf8.ValidString(key) {
		return errors.Errorf("invalid key [%x], must be a UTF-8 string", key)
	}
	if strings.HasPrefix(key, "_") {
		return errors.Errorf("invalid key [%s], cannot begin with \"_\"", key)
	}
	if key == "" {
		return errors.New("invalid key. Empty string is not supported as a key by mongodb")
	}
	return nil
}

// removeJSONRevision removes the "_rev" if this is a JSON
func removeJSONRevision(jsonValue *[]byte) error {
	jsonVal, err := castToJSON(*jsonValue)
	if err != nil {
		logger.Errorf("Failed to unmarshal mongodb JSON data: %+v", err)
		return err
	}
	jsonVal.removeRevField()
	if *jsonValue, err = jsonVal.toBytes(); err != nil {
		logger.Errorf("Failed to marshal mongodb JSON data: %+v", err)
	}
	return err
}
