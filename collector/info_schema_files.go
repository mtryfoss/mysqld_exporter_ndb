// Copyright 2018 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Scrape `information_schema.files`.

package collector

import (
	"context"
	"database/sql"

	"github.com/prometheus/client_golang/prometheus"
)

const infoSchemaFilesQuery = `
		SELECT TABLESPACE_NAME, LOGFILE_GROUP_NAME, ENGINE, FILE_TYPE, FILE_NAME
		   FREE_EXTENTS, TOTAL_EXTENTS, EXTENT_SIZE, INITIAL_SIZE, EXTRA
		  FROM information_schema.files
		  WHERE FILE_NAME != ""
		`

// Metric descriptors.
var (
	infoSchemaTableFilesFreeExtentsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, informationSchema, "files_free_extents"),
		"The number of extents which have not yet been used by the file",
		[]string{"tablespace", "logfileGroup", "engine", "fileType", "fileName", "extra"}, nil,
	)
	infoSchemaTableFilesTotalExtentsDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, informationSchema, "files_total_extents"),
		"The total number of extents allocated to the file",
		[]string{"tablespace", "logfileGroup", "engine", "fileType", "fileName", "extra"}, nil,
	)
	infoSchemaTableFilesExtentSizeDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, informationSchema, "files_extent_size"),
		"The size of an extent for the file in bytes",
		[]string{"tablespace", "logfileGroup", "engine", "fileType", "fileName", "extra"}, nil,
	)
	infoSchemaTableFilesInitialSizeDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, informationSchema, "files_initial_size"),
		"The size of the file in bytes",
		[]string{"tablespace", "logfileGroup", "engine", "fileType", "fileName", "extra"}, nil,
	)
)

// ScrapeFiles collects from `information_schema.files`.
type ScrapeFiles struct{}

// Name of the Scraper. Should be unique.
func (ScrapeFiles) Name() string {
	return "info_schema.files"
}

// Help describes the role of the Scraper.
func (ScrapeFiles) Help() string {
	return "If running with userstat=1, set to true to collect table statistics"
}

// Version of MySQL from which scraper is available.
func (ScrapeFiles) Version() float64 {
	return 5.1
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapeFiles) Scrape(ctx context.Context, db *sql.DB, ch chan<- prometheus.Metric) error {
	infoSchemaFilesRows, err := db.QueryContext(ctx, infoSchemaFilesQuery)
	if err != nil {
		return err
	}
	defer infoSchemaFilesRows.Close()

	var (
		tablespaceName, logfileGroupName      sql.NullString
		engine, fileType, fileName, extra     string
		freeExtents                           sql.NullInt64
		totalExtents, extentSize, initialSize uint64
	)

	for infoSchemaFilesRows.Next() {
		err = infoSchemaFilesRows.Scan(&tablespaceName, &logfileGroupName,
			&engine, &fileType, &fileName, &freeExtents, &totalExtents, &extentSize, &initialSize, &extra)
		if err != nil {
			return err
		}
		if freeExtents.Valid {
			ch <- prometheus.MustNewConstMetric(
				infoSchemaTableFilesFreeExtentsDesc, prometheus.GaugeValue, float64(freeExtents.Int64),
				tablespaceName.String, logfileGroupName.String, engine, fileType, fileName, extra,
			)
		}
		ch <- prometheus.MustNewConstMetric(
			infoSchemaTableFilesTotalExtentsDesc, prometheus.GaugeValue, float64(totalExtents),
			tablespaceName.String, logfileGroupName.String, engine, fileType, fileName, extra,
		)
		ch <- prometheus.MustNewConstMetric(
			infoSchemaTableFilesExtentSizeDesc, prometheus.GaugeValue, float64(extentSize),
			tablespaceName.String, logfileGroupName.String, engine, fileType, fileName, extra,
		)
		ch <- prometheus.MustNewConstMetric(
			infoSchemaTableFilesInitialSizeDesc, prometheus.GaugeValue, float64(initialSize),
			tablespaceName.String, logfileGroupName.String, engine, fileType, fileName, extra,
		)
	}
	return nil
}

// check interface
var _ Scraper = ScrapeFiles{}
