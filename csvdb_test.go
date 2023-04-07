package csvdb

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestUpsert(t *testing.T) {
	columns := []*Column{
		{Name: "id", Type: "string"},
		{Name: "name", Type: "string"},
	}

	table := &Table{
		Name:    "test_table",
		Columns: columns,
		Rows: []*Row{
			{Values: []string{"1", "Alice"}},
			{Values: []string{"2", "Bob"}},
		},
	}

	t.Run("Insert new row", func(t *testing.T) {
		newRow := []string{"3", "Charlie"}

		err := table.Upsert(newRow)
		if err != nil {
			t.Fatalf("Failed to upsert: %v", err)
		}

		if len(table.Rows) != 3 {
			t.Errorf("Expected table row count to be 3, got %d", len(table.Rows))
		}

		if !reflect.DeepEqual(table.Rows[2].Values, newRow) {
			t.Errorf("Expected last row values to be %v, got %v", newRow, table.Rows[2].Values)
		}
	})

	t.Run("Update existing row", func(t *testing.T) {
		updatedRow := []string{"2", "Updated Bob"}

		err := table.Upsert(updatedRow)
		if err != nil {
			t.Fatalf("Failed to upsert: %v", err)
		}

		if len(table.Rows) != 3 {
			t.Errorf("Expected table row count to be 3, got %d", len(table.Rows))
		}

		if !reflect.DeepEqual(table.Rows[1].Values, updatedRow) {
			t.Errorf("Expected second row values to be %v, got %v", updatedRow, table.Rows[1].Values)
		}
	})
}

func TestInsert(t *testing.T) {
	table := &Table{
		Name: "test_table",
		Columns: []*Column{
			{Name: "column1", Type: "int"},
			{Name: "column2", Type: "string"},
		},
		Rows: []*Row{
			{Values: []string{"1", "row1"}},
			{Values: []string{"2", "row2"}},
		},
	}

	err := table.Insert([]string{"3", "row3"})
	if err != nil {
		t.Errorf("Insert failed with error: %v", err)
	}
	expectedRows := []*Row{
		{Values: []string{"1", "row1"}},
		{Values: []string{"2", "row2"}},
		{Values: []string{"3", "row3"}},
	}
	if !reflect.DeepEqual(table.Rows, expectedRows) {
		t.Errorf("Incorrect rows after insert")
	}

	err = table.Insert([]string{"4"})
	if err == nil {
		t.Errorf("Insert should have failed")
	}
	if len(table.Rows) != 3 {
		t.Errorf("Rows should not have been appended")
	}
}

func TestExecute(t *testing.T) {
	db := &Database{
		Path:    "/test/db",
		Version: 1,
		Tables: map[string]*Table{
			"test_table": {
				Name: "test_table",
				Columns: []*Column{
					{Name: "id", Type: "int"},
					{Name: "name", Type: "string"},
				},
				Rows: []*Row{
					{Values: []string{"1", "Alice"}},
					{Values: []string{"2", "Bob"}},
					{Values: []string{"3", "Chuck"}},
				},
			},
		},
	}

	query := &Query{
		Type:    "select",
		Table:   "test_table",
		Columns: []string{"id", "name"},
	}

	rows, err := db.Execute(query)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	expectedRows := []*Row{
		{Values: []string{"1", "Alice"}},
		{Values: []string{"2", "Bob"}},
		{Values: []string{"3", "Chuck"}},
	}

	if !reflect.DeepEqual(rows, expectedRows) {
		t.Errorf("Incorrect rows")
	}

	query = &Query{
		Type:       "select",
		Table:      "test_table",
		Columns:    []string{"id", "name"},
		Conditions: map[string]string{"name": "Bob"},
	}

	rows, err = db.Execute(query)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	expectedRows = []*Row{
		{Values: []string{"2", "Bob"}},
	}

	if !reflect.DeepEqual(rows, expectedRows) {
		t.Errorf("Incorrect rows")
	}

	query = &Query{
		Type:    "select",
		Table:   "test_table",
		Columns: []string{"id", "name"},
		Limit:   1,
	}

	rows, err = db.Execute(query)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	expectedRows = []*Row{
		{Values: []string{"1", "Alice"}},
	}

	if !reflect.DeepEqual(rows, expectedRows) {
		t.Errorf("Incorrect rows")
	}

	query = &Query{
		Type:    "select",
		Table:   "test_table",
		Columns: []string{"id", "name"},
		Offset:  1,
	}

	rows, err = db.Execute(query)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	expectedRows = []*Row{
		{Values: []string{"2", "Bob"}},
		{Values: []string{"3", "Chuck"}},
	}

	if !reflect.DeepEqual(rows, expectedRows) {
		t.Errorf("Incorrect rows")
	}

	query = &Query{
		Type:       "select",
		Table:      "test_table",
		Columns:    []string{"id", "name"},
		Conditions: map[string]string{"name": "David"},
	}

	rows, err = db.Execute(query)
	if err != nil {
		t.Errorf("Execute should not have failed: %v", err)
	}

	expectedRows = []*Row{}

	if !reflect.DeepEqual(rows, expectedRows) {
		t.Errorf("Incorrect rows")
	}
}

func TestGetTable(t *testing.T) {
	tableName := "test_table"
	columns := []*Column{
		{Name: "column1", Type: "string"},
		{Name: "column2", Type: "string"},
		{Name: "column3", Type: "string"},
	}
	rows := []*Row{
		{Values: []string{"value1", "value2", "value3"}},
		{Values: []string{"value4", "value5", "value6"}},
	}

	table := &Table{
		Name:    tableName,
		Columns: columns,
		Rows:    rows,
	}

	db := &Database{
		Path:    "",
		Version: 1,
		Tables:  map[string]*Table{tableName: table},
	}

	t.Run("Existing table", func(t *testing.T) {
		result, err := db.GetTable(tableName)
		if err != nil {
			t.Fatalf("Failed to get table: %v", err)
		}

		if !reflect.DeepEqual(result, table) {
			t.Errorf("Incorrect table retrieved")
		}
	})

	t.Run("Non-existing table", func(t *testing.T) {
		_, err := db.GetTable("non_existing_table")
		if err == nil {
			t.Fatalf("Expected error for non-existing table, got nil")
		}

		expectedErrMsg := "table non_existing_table does not exist"
		if err.Error() != expectedErrMsg {
			t.Errorf("Expected error message '%s', got '%s'", expectedErrMsg, err.Error())
		}
	})
}

func TestDatabaseOpen(t *testing.T) {
	testDir, err := os.MkdirTemp("", "test_db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	versionContent := "1"
	err = os.WriteFile(filepath.Join(testDir, "version.txt"), []byte(versionContent), 0644)
	if err != nil {
		t.Fatal(err)
	}

	metadataContent := "table1,column1,string\ntable1,column2,string\ntable2,column1,string\n"
	err = os.WriteFile(filepath.Join(testDir, "metadata.csv"), []byte(metadataContent), 0644)
	if err != nil {
		t.Fatal(err)
	}
	tables := []struct {
		name    string
		content string
	}{
		{"table1", "column1,column2\n1,value1\n2,value2\n"},
		{"table2", "column1\nvalue3\nvalue4\n"},
	}
	os.Mkdir(filepath.Join(testDir, ".csvdb"), 0755)
	for _, table := range tables {
		tablePath := filepath.Join(testDir, ".csvdb", table.name)
		os.Mkdir(tablePath, 0755)
		err = os.WriteFile(filepath.Join(tablePath, "data.csv"), []byte(table.content), 0644)
		if err != nil {
			t.Fatal(err)
		}
	}
	db := &Database{}
	openedDB, err := db.Open(testDir)
	if err != nil {
		t.Fatal(err)
	}
	if openedDB.Path != testDir {
		t.Error("Incorrect Path")
	}
	if openedDB.Version != 1 {
		t.Error("Incorrect Version")
	}
	if len(openedDB.Tables) != 2 {
		t.Error("Incorrect number of tables")
	}
	table1 := openedDB.Tables["table1"]
	if table1.Name != "table1" {
		t.Error("Incorrect table1 Name")
	}
	if len(table1.Columns) != 2 || table1.Columns[0].Name != "column1" || table1.Columns[0].Type != "string" || table1.Columns[1].Name != "column2" || table1.Columns[1].Type != "string" {
		t.Error("Incorrect columns in table1")
	}
	if len(table1.Rows) != 2 || table1.Rows[0].Values[0] != "1" || table1.Rows[0].Values[1] != "value1" || table1.Rows[1].Values[0] != "2" || table1.Rows[1].Values[1] != "value2" {
		t.Error("Incorrect rows in table1")
	}
	table2 := openedDB.Tables["table2"]
	if table2.Name != "table2" {
		t.Error("Incorrect table2 Name")
	}
	if len(table2.Columns) != 1 || table2.Columns[0].Name != "column1" || table2.Columns[0].Type != "string" {
		t.Error("Incorrect columns in table2")
	}
	if len(table2.Rows) != 2 || table2.Rows[0].Values[0] != "value3" || table2.Rows[1].Values[0] != "value4" {
		t.Error("Incorrect rows in table2")
	}
}

func TestCreateTable(t *testing.T) {
	tempDir := os.TempDir()
	testDir := filepath.Join(tempDir, "csvdb_test")

	defer os.RemoveAll(testDir)

	db := &Database{
		Path:    testDir,
		Version: 1,
		Tables:  map[string]*Table{},
	}
	tableName := "test_table"
	columns := []string{"column1", "column2", "column3"}

	err := os.MkdirAll(filepath.Join(testDir, ".csvdb", tableName, ".csvdb", "data"), 0755)
	if err != nil {
		t.Fatalf("Failed to create necessary directories: %v", err)
	}

	metadataPath := filepath.Join(testDir, "metadata.csv")
	_, err = os.Create(metadataPath)
	if err != nil {
		t.Fatalf("Failed to create metadata file: %v", err)
	}

	err = db.CreateTable(tableName, columns)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	if _, ok := db.Tables[tableName]; !ok {
		t.Errorf("Table not added to database")
	}

	tablePath := getTablePath(db.Path, tableName)
	if _, err := os.Stat(tablePath); os.IsNotExist(err) {
		t.Errorf("Table directory not created")
	}

	tableFilePath := getCSVFilePath(tablePath, "data")
	if _, err := os.Stat(tableFilePath); os.IsNotExist(err) {
		t.Errorf("Table data file not created")
	}

	table := db.Tables[tableName]
	if len(table.Columns) != len(columns) {
		t.Errorf("Incorrect number of columns")
	}

	for i, column := range columns {
		if table.Columns[i].Name != column {
			t.Errorf("Incorrect column name")
		}
		if table.Columns[i].Type != "string" {
			t.Errorf("Incorrect column type")
		}
	}

	metadata, err := parseCSV(metadataPath)
	if err != nil {
		t.Fatalf("Failed to parse metadata: %v", err)
	}

	expectedMetadata := [][]string{
		{"test_table", "", ""},
	}

	if !reflect.DeepEqual(metadata, expectedMetadata) {
		t.Errorf("Incorrect metadata")
	}

	if db.Version != 2 {
		t.Errorf("Incorrect version")
	}
}
