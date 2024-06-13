package mysql

import (
	"database/sql"
	"fmt"
)

func connect(config Config) (*sql.DB, error) {
	dataSourceName := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s",
		config.User, config.Password, config.Host, config.Port, config.Database,
	)
	db, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection: %w", err)
	}
	return db, nil
}
