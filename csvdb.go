package csvdb

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type Database struct {
	Path    string
	Version int
	Tables  map[string]*Table
}

type Table struct {
	Name    string
	Columns []*Column
	Rows    []*Row
}

type Column struct {
	Name string
	Type string
}

type Row struct {
	Values []string
}

type Query struct {
	Type       string
	Table      string
	Columns    []string
	Conditions map[string]string
	Limit      int
	Offset     int
}

func (t *Table) Delete(conditions map[string]string) error {
	rowsToDelete := make([]*Row, 0)
	for _, row := range t.Rows {
		match := true
		for column, value := range conditions {
			columnIndex := -1
			for i, c := range t.Columns {
				if c.Name == column {
					columnIndex = i
					break
				}
			}
			if columnIndex == -1 {
				return fmt.Errorf("column %s not found", column)
			}
			if row.Values[columnIndex] != value {
				match = false
				break
			}
		}
		if match {
			rowsToDelete = append(rowsToDelete, row)
		}
	}

	for _, rowToDelete := range rowsToDelete {
		for i, row := range t.Rows {
			if row == rowToDelete {
				t.Rows = append(t.Rows[:i], t.Rows[i+1:]...)
				break
			}
		}
	}

	return nil
}

func (t *Table) Update(columns []string, values []string, conditions map[string]string) error {
	columnIndices := make([]int, len(columns))
	for i, column := range columns {
		found := false
		for j, tableColumn := range t.Columns {
			if column == tableColumn.Name {
				columnIndices[i] = j
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("column %s not found", column)
		}
	}
	for _, row := range t.Rows {
		matchesConditions := true
		for conditionColumn, conditionValue := range conditions {
			conditionIndex := -1
			for i, column := range t.Columns {
				if column.Name == conditionColumn {
					conditionIndex = i
					break
				}
			}
			if conditionIndex == -1 {
				return fmt.Errorf("condition column %s not found", conditionColumn)
			}
			if row.Values[conditionIndex] != conditionValue {
				matchesConditions = false
				break
			}
		}
		if matchesConditions {
			for i, column := range columnIndices {
				row.Values[column] = values[i]
			}
		}
	}
	return nil
}

func (t *Table) Select(columns []string, conditions map[string]string) ([]*Row, error) {
	colIndexes := make([]int, len(columns))
	for i, colName := range columns {
		found := false
		for j, col := range t.Columns {
			if col.Name == colName {
				colIndexes[i] = j
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("column %s not found", colName)
		}
	}
	var filteredRows []*Row
	for _, row := range t.Rows {
		matches := true
		for colName, value := range conditions {
			colIndex := -1
			for i, col := range t.Columns {
				if col.Name == colName {
					colIndex = i
					break
				}
			}
			if colIndex == -1 {
				return nil, fmt.Errorf("column %s not found in conditions", colName)
			}
			if row.Values[colIndex] != value {
				matches = false
				break
			}
		}
		if matches {
			filteredRows = append(filteredRows, row)
		}
	}
	projectedRows := make([]*Row, len(filteredRows))
	for i, row := range filteredRows {
		projectedValues := make([]string, len(columns))
		for j, colIndex := range colIndexes {
			projectedValues[j] = row.Values[colIndex]
		}
		projectedRows[i] = &Row{Values: projectedValues}
	}

	return projectedRows, nil
}

func (t *Table) getMatchingRow(row *Row) (*Row, error) {
	for _, existingRow := range t.Rows {
		if rowMatches(existingRow, row) {
			return existingRow, nil
		}
	}
	return nil, nil
}

func (t *Table) updateRow(existingRow *Row, row []string) {
	copy(existingRow.Values, row)
}

func (t *Table) Upsert(row []string) error {
	existingRow, err := t.getMatchingRow(&Row{Values: row})
	if err != nil {
		return err
	}
	if existingRow != nil {
		t.updateRow(existingRow, row)
		return nil
	}
	newRow := &Row{Values: row}
	t.Rows = append(t.Rows, newRow)
	return nil
}

func (t *Table) Insert(row []string) error {
	if len(row) != len(t.Columns) {
		return fmt.Errorf("invalid number of columns in row, expected %d got %d", len(t.Columns), len(row))
	}
	newRow := &Row{Values: row}
	t.Rows = append(t.Rows, newRow)
	return nil
}

func (db *Database) Execute(query *Query) ([]*Row, error) {
	table, err := db.GetTable(query.Table)
	if err != nil {
		return nil, err
	}
	for column := range query.Conditions {
		var found bool
		for _, tableColumn := range table.Columns {
			if tableColumn.Name == column {
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("invalid query condition: %s is not a valid column", column)
		}
	}
	rows, err := table.Select(query.Columns, query.Conditions)
	if err != nil {
		return nil, err
	}
	if query.Limit > 0 || query.Offset > 0 {
		limit := len(rows)
		if query.Limit > 0 && query.Limit < limit {
			limit = query.Limit
		}
		offset := 0
		if query.Offset > 0 && query.Offset < limit {
			offset = query.Offset
		}
		rows = rows[offset:limit]
	}
	return rows, nil
}

func (db *Database) GetTable(name string) (*Table, error) {
	if table, ok := db.Tables[name]; ok {
		return table, nil
	}
	return nil, fmt.Errorf("table %s does not exist", name)
}

func (db *Database) CreateTable(name string, columns []string) error {
	if _, ok := db.Tables[name]; ok {
		return fmt.Errorf("table %s already exists", name)
	}
	tablePath := getTablePath(db.Path, name)
	err := os.MkdirAll(tablePath, 0755)
	if err != nil {
		return err
	}
	tableFilePath := getCSVFilePath(tablePath, "data")
	file, err := os.Create(tableFilePath)
	if err != nil {
		return err
	}
	defer file.Close()
	header := strings.Join(columns, ",") + "\n"
	_, err = file.WriteString(header)
	if err != nil {
		return err
	}
	tableColumns := make([]*Column, len(columns))
	for i, columnName := range columns {
		tableColumns[i] = &Column{
			Name: columnName,
			Type: "string",
		}
	}
	table := &Table{
		Name:    name,
		Columns: tableColumns,
		Rows:    []*Row{},
	}
	db.Tables[name] = table
	metadataPath := filepath.Join(db.Path, "metadata.csv")
	version := db.Version
	metadata, err := parseCSV(metadataPath)
	if err != nil {
		return err
	}
	rowIndex := -1
	for i, row := range metadata {
		if row[0] == name {
			rowIndex = i
			break
		}
	}
	if rowIndex == -1 {
		rowIndex = len(metadata)
		metadata = append(metadata, []string{name, "", ""})
	}
	metadata[rowIndex][1] = ""
	metadata[rowIndex][2] = ""
	err = writeCSV(metadataPath, metadata)
	if err != nil {
		return err
	}
	db.Version = version + 1
	return nil
}

func (db *Database) Open(path string) (*Database, error) {
	_, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	versionPath := filepath.Join(path, "version.txt")
	versionContent, err := os.ReadFile(versionPath)
	if err != nil {
		return nil, err
	}
	version, err := strconv.Atoi(strings.TrimSpace(string(versionContent)))
	if err != nil {
		return nil, err
	}
	metadataPath := filepath.Join(path, "metadata.csv")
	metadataRows, err := parseCSV(metadataPath)
	if err != nil {
		return nil, err
	}
	tables := make(map[string]*Table)
	tablesPath := filepath.Join(path, ".csvdb")
	tablesDir, err := os.ReadDir(tablesPath)
	if err != nil {
		return nil, err
	}
	for _, tableDir := range tablesDir {
		if !tableDir.IsDir() {
			continue
		}
		tableName := tableDir.Name()
		tablePath := getCSVFilePath(path, tableName)
		tableRows, err := parseCSV(tablePath)
		if err != nil {
			return nil, err
		}
		columns := make([]*Column, len(tableRows[0]))
		for i, columnName := range tableRows[0] {
			columns[i] = &Column{
				Name: columnName,
				Type: "string",
			}
		}
		for _, metadataRow := range metadataRows {
			if metadataRow[0] == tableName && metadataRow[1] != "" {
				columnName := metadataRow[1]
				columnType := metadataRow[2]
				for _, column := range columns {
					if column.Name == columnName {
						column.Type = columnType
						break
					}
				}
			}
		}
		table := &Table{
			Name:    tableName,
			Columns: columns,
		}
		for _, rowValues := range tableRows[1:] {
			row := &Row{Values: rowValues}
			table.Rows = append(table.Rows, row)
		}

		tables[tableName] = table
	}
	database := &Database{
		Path:    path,
		Version: version,
		Tables:  tables,
	}
	return database, nil
}
