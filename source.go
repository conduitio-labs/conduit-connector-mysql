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

package mysql

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type Config struct {
	// The connection string for the MySQL database.
	DSN string `json:"dsn" validate:"required"`
}

type SourceConfig struct {
	sdk.DefaultSourceMiddleware

	Config

	// Holds the custom configuration that each table can have.
	TableConfig map[string]TableConfig `json:"tableConfig"`

	// Represents the tables to read from.
	//  - By default, no tables are included, but can be modified by adding a comma-separated string of regex patterns.
	//  - They are applied in the order that they are provided, so the final regex supersedes all previous ones.
	//  - To include all tables, use "*". You can then filter that list by adding a comma-separated string of regex patterns.
	//  - To set an "include" regex, add "+" or nothing in front of the regex.
	//  - To set an "exclude" regex, add "-" in front of the regex.
	//  - e.g. "-.*meta$, wp_postmeta" will exclude all tables ending with "meta" but include the table "wp_postmeta".
	Tables []string `json:"tables" validate:"required"`

	// Disables verbose cdc driver logs.
	DisableLogs bool `json:"cdc.disableLogs"`

	// Limits how many rows should be retrieved on each database fetch on snapshot mode.
	FetchSize uint64 `json:"snapshot.fetchSize" default:"10000"`

	// Removed. The unsafe snapshot no longer exists: true fails validation with a migration message, false is accepted and ignored. Tables without a primary key need tableConfig.<table>.sortingColumn.
	UnsafeSnapshot bool `json:"snapshot.unsafe"`

	// Controls whether the snapshot is done.
	SnapshotEnabled bool `json:"snapshot.enabled" default:"true"`

	// How column values become record values. `strict`: SQL NULL is null (nullable columns are
	// Avro unions with null), DECIMAL is exact, and a value that does not fit its schema type stops
	// the source; this is the certified configuration. `legacy`: the shapes of earlier versions
	// (NULL as the zero value, DECIMAL as double), deprecated. Unset: the mode the position was
	// written with, or `legacy` with a warning for a pipeline without a position. Changing it for a
	// pipeline that has a position needs `types.acceptChangeFrom`.
	TypesMode string `json:"types.mode"`

	// DECIMAL and NUMERIC in strict mode: `string` (the exact text, the default) or `double`
	// (lossy). Unset: the value the position was written with, else `string`.
	TypesDecimal string `json:"types.decimal"`

	// BIGINT UNSIGNED in strict mode: `long` (the default; a value above 9223372036854775807 stops
	// the source) or `string` (the full range, as decimal text). Unset: the value the position was
	// written with, else `long`.
	TypesBigintUnsigned string `json:"types.bigintUnsigned"`

	// Confirms a change of the type configuration of a pipeline that is in CDC: the name of the
	// configuration being replaced, as the refusal names it (for example `legacy` or
	// `strict/string/long`). Remove it once the log says a record under the new configuration was
	// acknowledged.
	TypesAcceptChangeFrom string `json:"types.acceptChangeFrom"`

	// rawTables holds the table rules as configured, before Open resolves
	// Tables to concrete table names.
	rawTables []string

	mysqlCfg *mysql.Config
}

func (s *SourceConfig) MysqlCfg() *mysql.Config {
	return s.mysqlCfg
}

func (s *SourceConfig) explicitTypes() explicitTypes {
	return explicitTypes{
		mode:             s.TypesMode,
		decimal:          s.TypesDecimal,
		bigintUnsigned:   s.TypesBigintUnsigned,
		acceptChangeFrom: s.TypesAcceptChangeFrom,
	}
}

func (s *SourceConfig) Validate(context.Context) error {
	if err := s.explicitTypes().validate(); err != nil {
		return err
	}

	if s.UnsafeSnapshot {
		return newError(ErrUnsafeSnapshotRemoved, CodeUnsafeSnapshotRemoved, "snapshot.unsafe",
			"remove snapshot.unsafe from the configuration (this applies to pipelines already in CDC too), then for each "+
				"table without a primary key either set tableConfig.<table>.sortingColumn to a column that is NOT NULL and the "+
				"only column of a primary key or unique index, add a primary key, exclude the table, or set snapshot.enabled=false",
			"the unsafe snapshot has been removed because it could not record its position or guarantee a consistent read")
	}

	mysqlCfg, err := mysql.ParseDSN(s.DSN)
	if err != nil {
		return fmt.Errorf("failed to parse DSN: %w", err)
	}

	// we need to take control over how do we handle time.Time values
	mysqlCfg.ParseTime = true

	s.mysqlCfg = mysqlCfg

	return nil
}

type TableConfig struct {
	// Allows to force using a custom column to sort the snapshot. The column must be
	// NOT NULL and the only column of a primary key or unique index.
	SortingColumn string `json:"sortingColumn"`
}

const (
	DefaultFetchSize = 50000
	// AllTablesWildcard can be used if you'd like to listen to all tables.
	AllTablesWildcard = "*"
)

type Source struct {
	sdk.UnimplementedSource

	config   SourceConfig
	db       *sqlx.DB
	iterator common.Iterator
	types    *typeSettings
}

const (
	defaultBatchDelay = time.Second
	defaultBatchSize  = 10000
)

func NewSource() sdk.Source {
	// The SDK decodes the configured sdk.batch.* values through these pointers,
	// so every Source needs its own copies. Pointing them at shared package
	// variables made one source's configuration overwrite another's (and raced
	// with the previous source's read loop).
	batchDelay := defaultBatchDelay
	batchSize := defaultBatchSize

	return sdk.SourceWithMiddleware(&Source{
		config: SourceConfig{
			DefaultSourceMiddleware: sdk.DefaultSourceMiddleware{
				SourceWithBatch: sdk.SourceWithBatch{
					BatchSize:  &batchSize,
					BatchDelay: &batchDelay,
				},
			},
		},
	})
}

func (s *Source) Config() sdk.SourceConfig {
	return &s.config
}

func (s *Source) Open(ctx context.Context, sdkPos opencdc.Position) (err error) {
	s.db, err = sqlx.Open("mysql", s.config.MysqlCfg().FormatDSN())
	if err != nil {
		return fmt.Errorf("failed to connect to mysql: %w", err)
	}

	// getAndFilterTables replaces Tables with the resolved names; keep the
	// configured rules to tell explicit lists from patterns.
	s.config.rawTables = slices.Clone(s.config.Tables)

	sdk.Logger(ctx).Info().Msg("Parsing table regexes...")
	s.config.Tables, err = s.getAndFilterTables(ctx, s.db, s.config.MysqlCfg().DBName)
	if err != nil {
		return err
	}

	canalRegexes := createCanalRegexes(s.config.MysqlCfg().DBName, s.config.Tables)

	sdk.Logger(ctx).Info().
		Strs("tables", s.config.Tables).
		Int("count", len(s.config.Tables)).
		Msgf("Successfully detected tables")

	serverID, err := common.GetServerID(ctx, s.db)
	if err != nil {
		return fmt.Errorf("failed to get server id: %w", err)
	}

	// set positions by default to nil, so that iterators know if starting from no position
	var pos common.Position
	if sdkPos != nil {
		parsed, err := common.ParseSDKPosition(sdkPos)
		if err != nil {
			// Covers both malformed JSON and common.ErrPositionVersionUnsupported
			// (a position written by a newer connector build than this one).
			return fmt.Errorf("bad source position given: %w", err)
		}
		pos = parsed
	}

	// A snapshot only runs when enabled and not resuming steady-state CDC. Only
	// then must every table have an orderable key, and a sorting column meet its
	// requirements. Pipelines already in steady-state CDC keep starting whatever
	// their snapshot config says.
	snapshotRuns := s.config.SnapshotEnabled && pos.CdcPosition == nil

	tableKeys, err := s.getTableKeys(s.config.MysqlCfg().DBName, snapshotRuns)
	if err != nil {
		return fmt.Errorf("failed to get table keys: %w", err)
	}

	s.types, err = s.openTypes(ctx, sdkPos != nil, pos)
	if err != nil {
		return err
	}

	if snapshotRuns {
		if err := s.validateSortingColumns(); err != nil {
			return err
		}
	}

	plan := resolveStartPlan(pos)

	coverage, err := resolveCoverage(plan, s.config.SnapshotEnabled, s.config.Tables, tablesAreExplicit(s.config.rawTables))
	if err != nil {
		return err
	}
	for _, w := range coverage.warnings {
		sdk.Logger(ctx).Warn().Msg(w)
	}
	switch plan.mode {
	case startModeCDCResume:
		sdk.Logger(ctx).Info().Msg("restart: resuming steady-state cdc from persisted position, skipping snapshot")
	case startModeSnapshotResume:
		sdk.Logger(ctx).Info().
			Str("cdc_start_binlog_file", plan.cdcPosition.Name).
			Uint32("cdc_start_binlog_pos", plan.cdcPosition.Pos).
			Msg("restart: resuming mid-snapshot from persisted cdc start position (P0)")
	default:
		sdk.Logger(ctx).Info().Msg("restart: no persisted cdc start position, falling back to a fresh capture")
	}

	s.iterator, err = newCombinedIterator(ctx, combinedIteratorConfig{
		db:                    s.db,
		tableKeys:             tableKeys,
		startSnapshotPosition: plan.snapshotPosition,
		startCdcPosition:      plan.cdcPosition,
		database:              s.config.MysqlCfg().DBName,
		canalRegexes:          canalRegexes,
		serverID:              serverID,
		mysqlConfig:           s.config.MysqlCfg(),
		disableCanalLogging:   s.config.DisableLogs,
		fetchSize:             s.config.FetchSize,
		// Issue #158: a position that already carries a cdc_position means the
		// snapshot completed in an earlier run; never run it again.
		snapshotEnabled: s.config.SnapshotEnabled && plan.mode != startModeCDCResume,
		coverage:        coverage.carry,
		types:           s.types,
	})
	if err != nil {
		return fmt.Errorf("failed to create snapshot iterator: %w", err)
	}
	sdk.Logger(ctx).Info().Msg("opened source connector")

	return nil
}

type startMode int

const (
	// startModeFresh: no usable persisted position, or a legacy snapshot
	// position without a P0. A fresh P0 is captured and the snapshot (if
	// enabled) starts over from the persisted per-table positions, if any.
	startModeFresh startMode = iota
	// startModeSnapshotResume: restart in the middle of (or at the very end of)
	// a snapshot whose P0 was persisted. Snapshot resumes from per-table
	// positions; CDC is seeded from P0.
	startModeSnapshotResume
	// startModeCDCResume: restart in steady-state CDC. The snapshot already
	// completed, so it must not run again.
	startModeCDCResume
)

// startPlan is what Open derives from the persisted position.
type startPlan struct {
	mode             startMode
	snapshotPosition *common.SnapshotPosition
	cdcPosition      *common.CdcPosition
}

// resolveStartPlan decides how a restart is seeded from the persisted position.
//
// Invariant 3: the branch taken here determines whether CDC resumes from a
// durable P0 or captures a fresh one; see
// docs/design-documents/20260724-snapshot-cdc-position-handoff.md.
//
// A cdc_position is only ever written by the CDC iterator, which starts after
// the snapshot has completed and every snapshot ack has been received. It is
// therefore proof the snapshot is done (issue #158). A snapshot_position, in
// contrast, means the snapshot may be incomplete and must be resumed, never
// skipped. Positions written before cdc_start or the version envelope existed
// decode into these same shapes (absent fields are nil/0).
func resolveStartPlan(pos common.Position) startPlan {
	switch {
	case pos.CdcPosition != nil:
		return startPlan{mode: startModeCDCResume, cdcPosition: pos.CdcPosition}

	case pos.SnapshotPosition != nil && pos.SnapshotPosition.CDCStart != nil:
		// Seed CDC from the persisted P0 (not a fresh master position) so the
		// binlog window (P0, restart] is replayed, and resume the snapshot from
		// its persisted per-table LastRead.
		return startPlan{
			mode:             startModeSnapshotResume,
			snapshotPosition: pos.SnapshotPosition,
			cdcPosition:      pos.SnapshotPosition.CDCStart,
		}

	default:
		// Fresh start, or a legacy position written before this fix existed
		// (pos.SnapshotPosition set, CDCStart nil). Capture a fresh P0 under the
		// lock; see the design doc's Failure modes section and Related section
		// (ConduitIO/conduit-connector-sdk#378).
		return startPlan{mode: startModeFresh, snapshotPosition: pos.SnapshotPosition}
	}
}

func (s *Source) ReadN(ctx context.Context, n int) ([]opencdc.Record, error) {
	//nolint:wrapcheck // error already wrapped in iterator
	return s.iterator.ReadN(ctx, n)
}

func (s *Source) Ack(ctx context.Context, _ opencdc.Position) error {
	//nolint:wrapcheck // error already wrapped in iterator
	return s.iterator.Ack(ctx, opencdc.Position{})
}

func (s *Source) Teardown(ctx context.Context) error {
	if s.iterator != nil {
		if err := s.iterator.Teardown(ctx); err != nil {
			//nolint:wrapcheck // error already wrapped in iterator
			return err
		}
	}

	if s.db != nil {
		if err := s.db.Close(); err != nil {
			return fmt.Errorf("failed to close connection: %w", err)
		}
	}

	return nil
}

func (s *Source) getAndFilterTables(ctx context.Context, db *sqlx.DB, database string) ([]string, error) {
	query := "SELECT table_name FROM information_schema.tables WHERE table_schema = ?"

	rows, err := db.Queryx(query, database)
	if err != nil {
		sdk.Logger(ctx).Error().Err(err).Msg("failed to query tables")
		return nil, fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, tableName)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	includedTables := make(map[string]bool)

	// Process each rule
	for _, rule := range s.config.Tables {
		if err := s.processTableRule(rule, tables, includedTables); err != nil {
			return nil, err
		}
	}

	var finalTables []string
	// Collect all the tables that were included (true in map)
	for table, included := range includedTables {
		if included {
			finalTables = append(finalTables, table)
		}
	}

	return finalTables, nil
}

// createCanalRegexes creates regex patterns for Canal from the filtered table names.
func createCanalRegexes(database string, tables []string) []string {
	canalRegexes := make([]string, 0, len(tables))
	for _, table := range tables {
		// prefix with db name because Canal does the same for the key, so we can't prefix with ^ to prevent undesired matches.
		// Append $ to prevent undesired matches.
		canalRegexes = append(canalRegexes, fmt.Sprintf("^%s.%s$", database, regexp.QuoteMeta(table)))
	}
	return canalRegexes
}

func (s *Source) processTableRule(rule string, tables []string, includedTables map[string]bool) error {
	// trim leading and trailing spaces from rule
	rule = strings.TrimSpace(rule)

	if rule == AllTablesWildcard {
		for _, table := range tables {
			includedTables[table] = true
		}
		return nil
	}

	action, regexPattern, err := ParseRule(rule)
	if err != nil {
		return err
	}

	// Compile the regex
	re, err := regexp.Compile(regexPattern)
	if err != nil {
		return fmt.Errorf("invalid regex pattern: %w", err)
	}

	// Apply the rule to all tables
	for _, table := range tables {
		if re.MatchString(table) {
			includedTables[table] = (action == Include)
		}
	}

	return nil
}

type Action int

const (
	Include Action = iota
	Exclude
)

// ParseRule parses a table filter rule and returns the action and pattern.
func ParseRule(rule string) (Action, string, error) {
	if len(rule) < 2 {
		return Include, "", fmt.Errorf("invalid rule format: %s", rule)
	}

	var action Action
	switch rule[0] {
	case '+':
		action = Include
		rule = rule[1:] // Strip the prefix
	case '-':
		action = Exclude
		rule = rule[1:] // Strip the prefix
	default:
		// No prefix means default to Include
		action = Include
	}

	return action, rule, nil // Return action and regex (without the action prefix)
}

// getTableKeys resolves the key columns used to order each table's snapshot.
// When requireKeys is false (no snapshot will run) tables without a key are
// allowed: CDC derives their record keys from the binlog position.
func (s *Source) getTableKeys(dbName string, requireKeys bool) (common.TableKeys, error) {
	tableKeys := make(common.TableKeys)

	for _, table := range s.config.Tables {
		preconfiguredTableKey, ok := s.config.TableConfig[table]
		if ok && preconfiguredTableKey.SortingColumn != "" {
			tableKeys[table] = common.PrimaryKeys{preconfiguredTableKey.SortingColumn}
			continue
		}

		primaryKeys, err := common.GetPrimaryKeys(s.db, dbName, table)
		if err != nil {
			return nil, err //nolint:wrapcheck // already wrapped
		}

		if len(primaryKeys) == 0 && requireKeys {
			return nil, newError(ErrTableWithoutKey, CodeTableWithoutKey, sortingColumnPath(table),
				"add a primary key; or set "+sortingColumnPath(table)+" to a column that is NOT NULL and the only column "+
					"of a primary key or unique index; or exclude the table with a pattern list such as "+
					`tables: "*,-^`+table+`$" (or drop it from an explicit list); or set snapshot.enabled=false to stream `+
					"changes only",
				"table %q has no primary key and no sorting column, so its snapshot cannot be ordered, resumed or read consistently", table)
		}

		tableKeys[table] = primaryKeys
	}

	return tableKeys, nil
}

// coverageDecision is the outcome of checking which tables a restart can
// trust to be covered by a snapshot.
type coverageDecision struct {
	// carry is stamped on every CDC position written by this run. nil keeps a
	// legacy position's "unknown".
	carry *common.TableCoverage
	// warnings are logged at WARN by Open.
	warnings []string
}

// tablesAreExplicit reports whether every table rule is a plain table name
// (an include with no regex metacharacters). Anything else, including "*" and
// an anchored name such as "^orders$", is a pattern that is allowed to match
// tables created later.
func tablesAreExplicit(rules []string) bool {
	if len(rules) == 0 {
		return false
	}
	for _, rule := range rules {
		rule = strings.TrimSpace(rule)
		if rule == AllTablesWildcard {
			return false
		}
		if strings.HasPrefix(rule, "-") {
			return false
		}
		rule = strings.TrimPrefix(rule, "+")
		if rule == "" || regexp.QuoteMeta(rule) != rule {
			return false
		}
	}
	return true
}

// resolveCoverage decides what Open does about tables that were never
// snapshotted. A snapshot only runs on a fresh start, so a table added to the
// configuration after the pipeline entered CDC (or a snapshot switched on for
// a pipeline that ran CDC-only) would otherwise have its pre-existing rows
// silently never delivered. Invariant 3: that is a loss path, so for an
// explicit table list it is an error, and for patterns it is a warning, issued
// once and then recorded (patterns match tables we cannot reasonably refuse to
// start over).
//
// The carried coverage is always restricted to the tables currently
// configured, so a table removed and added back later is treated as added.
//
// tables are the resolved table names; explicit says the configured rules are
// plain names. See docs/design-documents/20261012-cdc-snapshotted-tables.md.
func resolveCoverage(plan startPlan, snapshotEnabled bool, tables []string, explicit bool) (coverageDecision, error) {
	current := sortedUnique(tables)

	if plan.mode != startModeCDCResume {
		// A snapshot is about to run (or was disabled from the start). When it
		// runs, every current table is covered by the time CDC starts; when
		// disabled, nothing is.
		if snapshotEnabled {
			return coverageDecision{carry: common.NewTableCoverage(current, nil, nil)}, nil
		}
		return coverageDecision{carry: common.NewTableCoverage(nil, nil, nil)}, nil
	}

	persisted := plan.cdcPosition.TableCoverage

	if persisted == nil {
		// Legacy position: whether the tables were snapshotted is unknown.
		if !snapshotEnabled {
			return coverageDecision{}, nil
		}
		// Adopt the tables as unverified, never as snapshotted. Later additions
		// are detected against them; the warning repeats on every start.
		return coverageDecision{
			carry: common.NewTableCoverage(nil, current, nil),
			warnings: []string{fmt.Sprintf(
				"%s: restart from a cdc position written by an older version, which did not record which tables were "+
					"snapshotted. The tables in `tables` %v are ASSUMED to have been snapshotted but this cannot be "+
					"verified; if any was added after the pipeline entered CDC its existing rows were never delivered. "+
					"Tables added from now on are detected. Reset the position to snapshot everything again to clear this warning",
				CodeTablesUnverified, current)},
		}, nil
	}

	restricted := persisted.Restrict(current)
	decision := coverageDecision{carry: restricted}

	if restricted.HasUnverifiedTables() {
		unverified := fmt.Sprintf("%v", restricted.Unverified)
		if restricted.Hashed() {
			unverified = "(not listed: large table set)"
		}
		decision.warnings = append(decision.warnings, fmt.Sprintf(
			"%s: tables %s were adopted from a position written by an older version and could not be verified as "+
				"snapshotted; if any was added after the pipeline entered CDC its existing rows were never delivered. "+
				"Reset the position to snapshot everything again to clear this warning",
			CodeTablesUnverified, unverified))
	}

	if !snapshotEnabled {
		return decision, nil
	}

	added, changed := persisted.Added(current)
	if !changed {
		if persisted.HashStale() {
			// A legacy hash that was proven equal to the current set: carry
			// it forward under the current encoding so this check is not
			// repeated.
			decision.carry = persisted.WithCDCOnly(nil, current)
		}
		return decision, nil
	}

	if persisted.HashStale() {
		// The recorded hash was written by another encoding and could not be
		// shown equal to the configured set. This is not a statement that the
		// set changed, so say what is actually known.
		if explicit {
			return coverageDecision{}, newError(ErrTablesAddedAfterCDC, CodeTablesAddedAfterCDC, "tables",
				"reset the pipeline's position (this snapshots every table again; expect duplicates), or anchor the rules "+
					"as ^name$ (patterns); patterns only warn once and then record the current set. Restoring the `tables` "+
					"list does not help: the recorded hash cannot be compared",
				"the position records %d tables as a hash written by a different version of the connector (hash version %d, "+
					"this build uses %d) that cannot be matched against the %d configured tables, so it cannot be verified "+
					"that no table was added since the pipeline entered CDC. A table that was added would have its "+
					"existing rows silently skipped", persisted.Count, persisted.HashVersion, common.TableCoverageHashVersion, len(current))
		}
		decision.carry = persisted.WithCDCOnly(nil, current)
		decision.warnings = append(decision.warnings, fmt.Sprintf(
			"%s: the position records %d tables as a hash written by a different version of the connector, which cannot be "+
				"matched against the %d configured tables. Any table that was added since the pipeline entered CDC has its "+
				"existing rows NOT snapshotted and never delivered, only changes from now on are (CDC-only). This is logged "+
				"once. Reset the position to snapshot everything again",
			CodeTablesAddedAfterCDC, persisted.Count, len(current)))
		return decision, nil
	}

	if persisted.Hashed() {
		// The position stores only a hash, so the direction of the change is
		// unknown: a table may have been added or removed, and "remove them from
		// tables" cannot be offered as a fix.
		if explicit {
			return coverageDecision{}, newError(ErrTablesAddedAfterCDC, CodeTablesAddedAfterCDC, "tables",
				"restore the `tables` list this pipeline had when it entered CDC, or reset the pipeline's position "+
					"(this snapshots every table again; expect duplicates). If a plain name in `tables` matches more tables "+
					"than intended, anchor it as ^name$ (a pattern); patterns only warn",
				"the configured table set changed since this pipeline entered CDC (the position recorded %d tables as a "+
					"hash, the configuration now matches %d), and the difference cannot be named. A table that was added "+
					"would have its existing rows silently skipped", persisted.Count, len(current))
		}
		decision.carry = persisted.WithCDCOnly(nil, current)
		decision.warnings = append(decision.warnings, fmt.Sprintf(
			"%s: the configured table set changed since this pipeline entered CDC (the position recorded %d tables as a "+
				"hash, the configuration now matches %d), and the difference cannot be named. Any table that was added has "+
				"its existing rows NOT snapshotted and never delivered, only changes from now on are (CDC-only). This is "+
				"logged once. Reset the position to snapshot everything again",
			CodeTablesAddedAfterCDC, persisted.Count, len(current)))
		return decision, nil
	}

	if explicit {
		return coverageDecision{}, newError(ErrTablesAddedAfterCDC, CodeTablesAddedAfterCDC, "tables",
			"start a new pipeline for these tables, or reset this pipeline's position to snapshot everything again "+
				"(expect duplicates), or remove them from `tables`. If a plain name in `tables` matches more tables than "+
				"intended, anchor it as ^name$ (a pattern); patterns only warn and the new tables are then delivered "+
				"from CDC only",
			"%s were added after this pipeline entered CDC, so their existing rows were never snapshotted "+
				"and would be silently skipped", fmt.Sprintf("tables %v", added))
	}

	// Patterns: warn once, then record the tables so the warning does not repeat.
	decision.carry = persisted.WithCDCOnly(added, current)
	decision.warnings = append(decision.warnings, fmt.Sprintf(
		"%s: %s match `tables` but were added after this pipeline entered CDC; their existing rows are NOT "+
			"snapshotted and will not be delivered, only changes from now on are (CDC-only). This is logged once. "+
			"Start a new pipeline or reset the position to snapshot them",
		CodeTablesAddedAfterCDC, fmt.Sprintf("tables %v", added)))
	return decision, nil
}

func sortedUnique(in []string) []string {
	out := slices.Clone(in)
	slices.Sort(out)
	out = slices.Compact(out)
	if out == nil {
		out = []string{}
	}
	return out
}
