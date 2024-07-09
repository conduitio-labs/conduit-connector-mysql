package common

type (
	PrimaryKeyName string
	TableName      string
	TableKeys      map[TableName]PrimaryKeyName
)
