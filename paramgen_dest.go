// Code generated by paramgen. DO NOT EDIT.
// Source: github.com/ConduitIO/conduit-commons/tree/main/paramgen

package mysql

import (
	"github.com/conduitio/conduit-commons/config"
)

const (
	DestinationConfigDestinationConfigParam = "destinationConfigParam"
	DestinationConfigUrl                    = "url"
)

func (DestinationConfig) Parameters() map[string]config.Parameter {
	return map[string]config.Parameter{
		DestinationConfigDestinationConfigParam: {
			Default:     "yes",
			Description: "DestinationConfigParam must be either yes or no (defaults to yes).",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationInclusion{List: []string{"yes", "no"}},
			},
		},
		DestinationConfigUrl: {
			Default:     "",
			Description: "URL is the connection string for the Mysql database.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
	}
}
