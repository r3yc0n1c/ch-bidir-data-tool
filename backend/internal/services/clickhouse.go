package services

import (
	"context"
	"fmt"
	"strings"

	"clickhouse-integration/internal/models"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type ClickHouseService struct{}

func NewClickHouseService() *ClickHouseService {
	return &ClickHouseService{}
}

func (s *ClickHouseService) Connect(config models.ClickHouseConfig) (driver.Conn, error) {
	// For Docker, use native protocol by default (port 9000)
	if config.Port == 0 {
		config.Port = 9000 // Default native protocol port
	}

	// Force IPv4 address
	host := config.Host
	if host == "localhost" || host == "::1" {
		host = "127.0.0.1"
	}

	opts := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", host, config.Port)},
		Auth: clickhouse.Auth{
			Database: config.Database,
			Username: "default",
			Password: "password", // Use the password from docker-compose.yml
		},
		Debug: true,
		Settings: map[string]interface{}{
			"max_execution_time": 60,
		},
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %v", err)
	}

	if err := conn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %v", err)
	}

	return conn, nil
}

func (s *ClickHouseService) GetTables(conn driver.Conn, database string) ([]string, error) {
	fmt.Printf("Querying tables for database: %s\n", database)
	query := fmt.Sprintf("SELECT name FROM system.tables WHERE database = '%s'", database)
	fmt.Printf("Executing query: %s\n", query)

	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %v", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %v", err)
		}
		tables = append(tables, table)
		fmt.Printf("Found table: %s\n", table)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating tables: %v", err)
	}

	fmt.Printf("Total tables found: %d\n", len(tables))
	return tables, nil
}

func (s *ClickHouseService) GetColumns(conn driver.Conn, database, table string) ([]models.Column, error) {
	query := fmt.Sprintf(`
		SELECT name, type, is_nullable
		FROM system.columns
		WHERE database = '%s' AND table = '%s'
	`, database, table)

	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns: %v", err)
	}
	defer rows.Close()

	var columns []models.Column
	for rows.Next() {
		var col models.Column
		var isNullable string
		if err := rows.Scan(&col.Name, &col.Type, &isNullable); err != nil {
			return nil, fmt.Errorf("failed to scan column: %v", err)
		}
		col.Nullable = isNullable == "1"
		columns = append(columns, col)
	}

	return columns, nil
}

func (s *ClickHouseService) ExportData(conn driver.Conn, req models.ExportRequest) ([][]interface{}, error) {
	fmt.Printf("Exporting data from table: %s\n", req.Table)
	fmt.Printf("Selected columns: %v\n", req.Columns)

	columns := strings.Join(req.Columns, ", ")
	query := fmt.Sprintf("SELECT %s FROM %s", columns, req.Table)
	if req.Query != "" {
		query = req.Query
	}
	fmt.Printf("Executing query: %s\n", query)

	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %v", err)
	}
	defer rows.Close()

	var results [][]interface{}
	rowCount := 0
	for rows.Next() {
		row := make([]interface{}, len(req.Columns))
		rowPtrs := make([]interface{}, len(req.Columns))
		for i := range row {
			rowPtrs[i] = &row[i]
		}
		if err := rows.Scan(rowPtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}
		results = append(results, row)
		rowCount++
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %v", err)
	}

	fmt.Printf("Total rows exported: %d\n", rowCount)
	return results, nil
}

func (s *ClickHouseService) CreateTable(conn driver.Conn, tableName string, columns []models.Column) error {
	// Build column definitions
	var columnDefs []string
	for _, col := range columns {
		nullable := ""
		if col.Nullable {
			nullable = "Nullable"
		}
		columnDefs = append(columnDefs, fmt.Sprintf("%s %s(%s)", col.Name, nullable, col.Type))
	}

	// Create table query
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			%s
		) ENGINE = MergeTree()
		ORDER BY tuple()
	`, tableName, strings.Join(columnDefs, ",\n"))

	if err := conn.Exec(context.Background(), query); err != nil {
		return fmt.Errorf("failed to create table: %v", err)
	}

	return nil
}

func (s *ClickHouseService) ImportData(conn driver.Conn, req models.ImportRequest) error {
	fmt.Printf("Starting import process for table: %s\n", req.Table)
	fmt.Printf("Columns to import: %+v\n", req.Columns)
	fmt.Printf("Number of rows to import: %d\n", len(req.Data))

	// First, create the table if it doesn't exist
	if err := s.CreateTable(conn, req.Table, req.Columns); err != nil {
		return fmt.Errorf("failed to prepare table: %v", err)
	}

	// Extract column names for the INSERT query
	columnNames := make([]string, len(req.Columns))
	for i, col := range req.Columns {
		columnNames[i] = col.Name
	}

	columns := strings.Join(columnNames, ", ")
	placeholders := strings.Repeat("?,", len(req.Columns)-1) + "?"

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", req.Table, columns, placeholders)
	fmt.Printf("Preparing batch insert with query: %s\n", query)

	batch, err := conn.PrepareBatch(context.Background(), query)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %v", err)
	}

	fmt.Printf("Starting to append %d rows to batch\n", len(req.Data))
	for i, row := range req.Data {
		if i%1000 == 0 {
			fmt.Printf("Processing row %d\n", i)
		}
		if err := batch.Append(row...); err != nil {
			return fmt.Errorf("failed to append row %d: %v", i, err)
		}
	}

	fmt.Printf("Sending batch to ClickHouse\n")
	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %v", err)
	}

	// Verify the import by counting rows
	countQuery := fmt.Sprintf("SELECT count() FROM %s", req.Table)
	fmt.Printf("Verifying import with query: %s\n", countQuery)

	rows, err := conn.Query(context.Background(), countQuery)
	if err != nil {
		return fmt.Errorf("failed to verify import: %v", err)
	}
	defer rows.Close()

	var count uint64
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			return fmt.Errorf("failed to scan row count: %v", err)
		}
	}
	fmt.Printf("Successfully imported data. Total rows in table %s: %d\n", req.Table, count)

	return nil
}
