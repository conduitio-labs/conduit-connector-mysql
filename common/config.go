// Copyright © 2024 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"fmt"
	"strings"
)

type Config struct {
	Host     string `json:"host" validate:"required"`
	Port     int    `json:"port" default:"3306"`
	User     string `json:"user" validate:"required"`
	Password string `json:"password" validate:"required"`
	Database string `json:"database" validate:"required"`
}

//go:generate paramgen -output=paramgen_src.go SourceConfig

type SourceConfig struct {
	Config

	Tables []string `json:"tables" validate:"required"`
}

func (config SourceConfig) ToMap() map[string]string {

	return map[string]string{
		"host":     config.Host,
		"port":     fmt.Sprint(config.Port),
		"user":     config.User,
		"password": config.Password,
		"database": config.Database,
		"tables":   strings.Join((config.Tables), ","),
	}
}
