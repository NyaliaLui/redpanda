// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package schema

import (
	"fmt"
	"os"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/schemaregistry"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/sr"
)

func newCheckCompatibilityCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var schemaFile, schemaType, sversion string
	cmd := &cobra.Command{
		Use:   "check-compatibility SUBJECT",
		Short: "Check schema compatibility with existing schemas in the subject",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			f := p.Formatter
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			cl, err := schemaregistry.NewClient(fs, p)
			out.MaybeDie(err, "unable to initialize schema registry client: %v", err)

			version, err := parseVersion(sversion)
			out.MaybeDieErr(err)

			file, err := os.ReadFile(schemaFile)
			out.MaybeDie(err, "unable to read %q: %v", schemaFile, err)

			t, err := resolveSchemaType(schemaType, schemaFile)
			out.MaybeDieErr(err)

			subject := args[0]
			schema := sr.Schema{
				Schema: string(file),
				Type:   t,
			}
			compatible, err := cl.CheckCompatibility(cmd.Context(), subject, version, schema)
			out.MaybeDie(err, "unable to check compatibility: %v", err)
			type res struct {
				Compatible bool `json:"compatible" yaml:"compatible"`
			}
			if isText, _, s, err := f.Format(res{compatible}); !isText {
				out.MaybeDie(err, "unable to print in the required format %q: %v", f.Kind, err)
				out.Exit(s)
			}
			if compatible {
				fmt.Println("Schema is compatible.")
			} else {
				fmt.Println("Schema is not compatible.")
			}
		},
	}

	cmd.Flags().StringVar(&schemaFile, "schema", "", "Schema filepath to check, must be .avro or .proto")
	cmd.Flags().StringVar(&schemaType, "type", "", "Schema type, one of protobuf or avro; overrides schema file extension")
	cmd.Flags().StringVar(&sversion, "schema-version", "", "Schema version to check compatibility with (latest, 0, 1...)")

	cmd.MarkFlagRequired("schema")
	cmd.MarkFlagRequired("schema-version")
	return cmd
}
