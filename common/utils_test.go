package common

import (
	"testing"

	"github.com/matryer/is"
)

func TestParseVersion(t *testing.T) {
	is := is.New(t)

	for _, testcase := range []struct {
		in          string
		wantFlavor  string
		wantVersion string
	}{
		// MySQL

		// Plain GA ‑ just X.Y.Z
		{"8.0.32", "mysql", "8.0.32"},
		// ‑log build tag (community & distro builds)
		{"5.7.44-log", "mysql", "5.7.44"},
		// Distro / package suffixes (‑0ubuntu…, +deb…, epoch 1: etc.)
		{"8.0.37-1ubuntu20.04", "mysql", "8.0.37"},
		// Enterprise (‑enterprise‑commercial‑advanced)
		{"5.6.31-enterprise-commercial-advanced-log", "mysql", "5.6.31"},
		// Percona Server (‑Percona‑Server)
		{"8.0.36-28-Percona-Server", "mysql", "8.0.36"},

		// MariaDB

		// MariaDB normal (‑MariaDB…)
		{"10.11.5-MariaDB-1:10.11.5+maria~ubu2204", "mariadb", "10.11.5"},
		// MariaDB with ‑log
		{"10.6.14-MariaDB-log", "mariadb", "10.6.14"},
		// MariaDB “compatibility prefix” 5.5.5‑
		{"5.5.5-10.6.8-MariaDB", "mariadb", "10.6.8"},
		// Prefix + log (worst‑case combo)
		{"5.5.5-10.6.14-MariaDB-log", "mariadb", "10.6.14"},
	} {
		version, flavor, err := parseVersion(testcase.in)
		is.NoErr(err)
		is.Equal(flavor, testcase.wantFlavor)
		is.Equal(version, testcase.wantVersion)
	}
}

func TestGetShowBinaryLogQuery(t *testing.T) {
	is := is.New(t)

	for _, testcase := range []struct {
		flavor        string
		serverVersion string
		expected      string
	}{
		{flavor: "mariadb", serverVersion: "10.5.2", expected: "SHOW BINLOG STATUS"},
		{flavor: "mariadb", serverVersion: "10.6.0", expected: "SHOW BINLOG STATUS"},
		{flavor: "mariadb", serverVersion: "10.4.0", expected: "SHOW MASTER STATUS"},
		{flavor: "mysql", serverVersion: "8.4.0", expected: "SHOW BINARY LOG STATUS"},
		{flavor: "mysql", serverVersion: "8.4.1", expected: "SHOW BINARY LOG STATUS"},
		{flavor: "mysql", serverVersion: "8.0.33", expected: "SHOW MASTER STATUS"},
		{flavor: "mysql", serverVersion: "5.7.41", expected: "SHOW MASTER STATUS"},
		{flavor: "other", serverVersion: "1.0.0", expected: "SHOW MASTER STATUS"},
	} {
		got := getShowBinaryLogQuery(testcase.flavor, testcase.serverVersion)
		is.Equal(testcase.expected, got)
	}
}
