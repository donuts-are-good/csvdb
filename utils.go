package csvdb

import (
	"encoding/csv"
	"os"
	"path/filepath"
)

// func getPrimaryKey(table *Table) *Column {
// 	for _, col := range table.Columns {
// 		if col.Type == "primary" {
// 			return col
// 		}
// 	}
// 	return nil
// }

func rowMatches(row1 *Row, row2 *Row) bool {
	if len(row1.Values) != len(row2.Values) {
		return false
	}
	for i, value := range row1.Values {
		if value != row2.Values[i] {
			return false
		}
	}
	return true
}

func writeCSV(filename string, rows [][]string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	return writer.WriteAll(rows)
}

func parseCSV(filename string) ([][]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	rows, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func getCSVFilePath(databasePath string, tableName string) string {
	return filepath.Join(databasePath, ".csvdb", tableName, "data.csv")
}

func getTablePath(databasePath string, tableName string) string {
	return filepath.Join(databasePath, ".csvdb", tableName)
}
