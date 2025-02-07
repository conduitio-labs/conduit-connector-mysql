// Code generated by paramgen. DO NOT EDIT.
// Source: github.com/ConduitIO/conduit-commons/tree/main/paramgen

package common

import (
	"github.com/conduitio/conduit-commons/config"
)

const (
	SourceConfigDisableCanalLogs               = "disableCanalLogs"
	SourceConfigDsn                            = "dsn"
	SourceConfigFetchSize                      = "fetchSize"
	SourceConfigSdkBatchDelay                  = "sdk.batch.delay"
	SourceConfigSdkBatchSize                   = "sdk.batch.size"
	SourceConfigSdkSchemaContextEnabled        = "sdk.schema.context.enabled"
	SourceConfigSdkSchemaContextName           = "sdk.schema.context.name"
	SourceConfigSdkSchemaExtractKeyEnabled     = "sdk.schema.extract.key.enabled"
	SourceConfigSdkSchemaExtractKeySubject     = "sdk.schema.extract.key.subject"
	SourceConfigSdkSchemaExtractPayloadEnabled = "sdk.schema.extract.payload.enabled"
	SourceConfigSdkSchemaExtractPayloadSubject = "sdk.schema.extract.payload.subject"
	SourceConfigSdkSchemaExtractType           = "sdk.schema.extract.type"
	SourceConfigTableConfigSortingColumn       = "tableConfig.*.sortingColumn"
	SourceConfigTables                         = "tables"
	SourceConfigUnsafeSnapshot                 = "unsafeSnapshot"
)

func (SourceConfig) Parameters() map[string]config.Parameter {
	return map[string]config.Parameter{
		SourceConfigDisableCanalLogs: {
			Default:     "",
			Description: "DisableCanalLogs disables verbose logs.",
			Type:        config.ParameterTypeBool,
			Validations: []config.Validation{},
		},
		SourceConfigDsn: {
			Default:     "",
			Description: "DSN is the connection string for the MySQL database.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
		SourceConfigFetchSize: {
			Default:     "10000",
			Description: "FetchSize limits how many rows should be retrieved on each database fetch.",
			Type:        config.ParameterTypeInt,
			Validations: []config.Validation{},
		},
		SourceConfigSdkBatchDelay: {
			Default:     "0",
			Description: "Maximum delay before an incomplete batch is read from the source.",
			Type:        config.ParameterTypeDuration,
			Validations: []config.Validation{
				config.ValidationGreaterThan{V: -1},
			},
		},
		SourceConfigSdkBatchSize: {
			Default:     "0",
			Description: "Maximum size of batch before it gets read from the source.",
			Type:        config.ParameterTypeInt,
			Validations: []config.Validation{
				config.ValidationGreaterThan{V: -1},
			},
		},
		SourceConfigSdkSchemaContextEnabled: {
			Default:     "true",
			Description: "Specifies whether to use a schema context name. If set to false, no schema context name will\nbe used, and schemas will be saved with the subject name specified in the connector\n(not safe because of name conflicts).",
			Type:        config.ParameterTypeBool,
			Validations: []config.Validation{},
		},
		SourceConfigSdkSchemaContextName: {
			Default:     "",
			Description: "Schema context name to be used. Used as a prefix for all schema subject names.\nIf empty, defaults to the connector ID.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		SourceConfigSdkSchemaExtractKeyEnabled: {
			Default:     "true",
			Description: "Whether to extract and encode the record key with a schema.",
			Type:        config.ParameterTypeBool,
			Validations: []config.Validation{},
		},
		SourceConfigSdkSchemaExtractKeySubject: {
			Default:     "key",
			Description: "The subject of the key schema. If the record metadata contains the field\n\"opencdc.collection\" it is prepended to the subject name and separated\nwith a dot.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		SourceConfigSdkSchemaExtractPayloadEnabled: {
			Default:     "true",
			Description: "Whether to extract and encode the record payload with a schema.",
			Type:        config.ParameterTypeBool,
			Validations: []config.Validation{},
		},
		SourceConfigSdkSchemaExtractPayloadSubject: {
			Default:     "payload",
			Description: "The subject of the payload schema. If the record metadata contains the\nfield \"opencdc.collection\" it is prepended to the subject name and\nseparated with a dot.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		SourceConfigSdkSchemaExtractType: {
			Default:     "avro",
			Description: "The type of the payload schema.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationInclusion{List: []string{"avro"}},
			},
		},
		SourceConfigTableConfigSortingColumn: {
			Default:     "",
			Description: "SortingColumn allows to force using a custom column to sort the snapshot.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		SourceConfigTables: {
			Default:     "",
			Description: "Tables to be snapshotted. By default, all tables are included, but can be modified by adding a comma-separated string of regex patterns. They are applied in the order that they are provided, so the final regex supersedes all previous ones. To set an \"include\" regex, add \"+\" or nothing in front of the regex. To set an \"exclude\" regex, add \"-\" in front of the regex. e.g. \"-.*meta$, wp_postmeta\" will include all tables (by default), but exclude all tables ending with \"meta\" but include the table \"wp_postmeta\"",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		SourceConfigUnsafeSnapshot: {
			Default:     "",
			Description: "UnsafeSnapshot allows a snapshot of a table with neither a primary key\nnor a defined sorting column. The opencdc.Position won't record the last record\nread from a table.",
			Type:        config.ParameterTypeBool,
			Validations: []config.Validation{},
		},
	}
}
