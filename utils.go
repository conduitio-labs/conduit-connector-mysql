package mysql

import (
	"fmt"

	"github.com/jmoiron/sqlx"
)

func connect(config Config) (*sqlx.DB, error) {
	dataSourceName := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s",
		config.User, config.Password, config.Host, config.Port, config.Database,
	)
	db, err := sqlx.Open("mysql", dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection: %w", err)
	}
	return db, nil
}
