# CSVDB
**WARNING:** This project doesn't have full testing yet (check `csvdb_test.go`) and should not be taken seriously or used in production. It's an experimental project being done to figure out "What if?".

## what?
CSVDB is a flat file database that uses CSV files as storage. Each table is a CSV file with a header row containing the column names and types like `name (type`). This project is not meant to be a production-ready database, but rather an exploration of what could be achieved using CSV files as a storage mechanism.

## Installation
Clone the repository from GitHub:

```bash
git clone https://github.com/donuts-are-good/csvdb.git
```

## Usage
```go
import (
	"github.com/donuts-are-good/csvdb"
)
```

## Setting up the Database
When you open a database using csvdb.Open, the function will create a new directory at the specified path if it doesn't already exist. This directory will store the database files, including the metadata and the table files as CSVs.

### To set up a new database:

Choose a directory where you want to store the database files. If the directory doesn't exist, the csvdb.Open function will create it for you.
```go
dbPath := "path/to/database"
```
Open the database using the csvdb.Open function. If the directory doesn't exist, this function will create it and initialize the necessary metadata files. If the directory already exists and contains a valid CSVDB structure, it will open the existing database.
```go
db, err := csvdb.Open(dbPath)
if err != nil {
    // Handle error
}
```
Once the database is opened or created, you can create new tables, insert rows, and perform other operations as demonstrated in the usage examples below.


## Open/Create Database
```go
db, err := csvdb.Open("path/to/database")
if err != nil {
	// Handle error
}
```
## Create Table
```go
err := db.CreateTable("table_name", []string{"column1", "column2"})
if err != nil {
	// Handle error
}
```

## Get Table
```go
table, err := db.GetTable("table_name")
if err != nil {
	// Handle error
}
```
## Insert Row
```go
err := table.Insert([]string{"value1", "value2"})
if err != nil {
	// Handle error
}
```
## Upsert Row
```go
err := table.Upsert([]string{"value1", "value2"})
if err != nil {
	// Handle error
}
```

## Update Rows
```go
err := table.Update([]string{"column1"}, []string{"new_value1"}, map[string]string{"column2": "value2"})
if err != nil {
	// Handle error
}
```
## Delete Rows
```go
err := table.Delete(map[string]string{
  "column1": "value1", 
  "column2": "value2",
  })
if err != nil {
	// Handle error
}
```
## Select Rows
```go
rows, err := table.Select([]string{"column1", "column2"}, map[string]string{"column1": "value1"})
if err != nil {
	// Handle error
}
```
## Execute Query
```go
query := &csvdb.Query{
	Type:       "SELECT",
	Table:      "table_name",
	Columns:    []string{"column1", "column2"},
	Conditions: map[string]string{"column1": "value1"},
	Limit:      10,
	Offset:     0,
}

rows, err := db.Execute(query)
if err != nil {
	// Handle error
}
``````
## Donut Shop Demo 

```go
package main

import (
	"fmt"
	"github.com/donuts-are-good/csvdb"
)

func main() {
	dbPath := "path/to/database"
	db, err := csvdb.Open(dbPath)
	if err != nil {
		fmt.Println("Error opening database:", err)
		return
	}

	err = db.CreateTable("donuts", []string{"id", "name", "price"})
	if err != nil {
		fmt.Println("Error creating table:", err)
		return
	}

	table, err := db.GetTable("donuts")
	if err != nil {
		fmt.Println("Error getting table:", err)
		return
	}

	// Add new donuts
	err = table.Insert([]string{"1", "Glazed", "1.00"})
	if err != nil {
		fmt.Println("Error inserting row:", err)
		return
	}

	err = table.Insert([]string{"2", "Chocolate", "1.50"})
	if err != nil {
		fmt.Println("Error inserting row:", err)
		return
	}

	// Update an existing donut
	err = table.Update([]string{"price"}, []string{"1.25"}, map[string]string{"id": "1"})
	if err != nil {
		fmt.Println("Error updating row:", err)
		return
	}

	// List all available donuts
	rows, err := table.Select([]string{"id", "name", "price"}, map[string]string{})
	if err != nil {
		fmt.Println("Error selecting rows:", err)
		return
	}

	fmt.Println("Available Donuts:")
	for _, row := range rows {
		fmt.Printf("ID: %s, Name: %s, Price: $%s\n", row.Values[0], row.Values[1], row.Values[2])
	}

	// Delete a donut
	err = table.Delete(map[string]string{"id": "2"})
	if err != nil {
		fmt.Println("Error deleting row:", err)
		return
	}

	// List available donuts after deletion
	rows, err = table.Select([]string{"id", "name", "price"}, map[string]string{})
	if err != nil {
		fmt.Println("Error selecting rows:", err)
		return
	}

	fmt.Println("\nAvailable Donuts after deletion:")
	for _, row := range rows {
		fmt.Printf("ID: %s, Name: %s, Price: $%s\n", row.Values[0], row.Values[1], row.Values[2])
	}
}

```
## Contributing
If you're interested in contributing to this project or have any questions, please feel free to open an issue or submit a pull request. Keep in mind that this project is not meant for production use, and its primary purpose is exploration and experimentation.

## License
This project is licensed under the MIT License.
