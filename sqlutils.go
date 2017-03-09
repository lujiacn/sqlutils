//Package sqlutils supply the functions convert sql.Rows to map, RawData Struct, arry, mapjson, arrayjson, csv
package sqlutils

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"

	"gopkg.in/rana/ora.v4"
)

//RawData store header and rows
type RawData struct {
	Header []string
	Rows   []interface{}
}

//RowToJSON conver sql.Rows to JSON format{"Header":["1+2"],"Rows":[[["3"]]]}
func RowToArrayJSON(rows *sql.Rows) string {
	record := RowToRawData(rows)
	b, err := json.Marshal(record)
	if err != nil {
		fmt.Print("%v", err)
	}
	return string(b)
}

//RowToCSV conver sql.Rows to CSV string including header
func RowToCSV(rows *sql.Rows) (string, error) {
	data, err := RowToArr(rows)
	if err != nil {
		return "", err
	}
	b := &bytes.Buffer{}
	writer := csv.NewWriter(b)
	writer.WriteAll(data)
	writer.Flush()
	return string(b.Bytes()), nil
}

//RowtoMapJSON with format {[{"1+2":3, "3+4": 7}]}
func RowToMapJSON(rows *sql.Rows) string {
	record := RowToMap(rows)
	b, err := json.Marshal(record)
	if err != nil {
		return fmt.Sprintf("{'error': '%v'}", err)
	}
	return string(b)
}

func switchType(val interface{}) string {
	var result string
	switch val.(type) {
	case int, int32, int64, float64:
		result = fmt.Sprintf("%v", val)
	case ora.Lob:
		newVal, ok := val.(ora.Lob)
		if ok && newVal.Reader != nil {
			b, err := ioutil.ReadAll(newVal)
			if err != nil {
				result = fmt.Sprintf("%v", err)
			} else {
				result = string(b)
			}
		} else {
			result = ""
		}
		newVal.Close()
	default:
		result = fmt.Sprintf("%s", val)
	}
	return result
}

func assertTypeMap(cols []string, rawCols []interface{}) map[string]string {
	resultCols := make(map[string]string, len(cols))
	for i, c := range cols {
		val := rawCols[i]
		if val == nil {
			resultCols[c] = ""
		} else {
			resultCols[c] = switchType(val)
		}
	}
	return resultCols
}

func assertTypeArray(cols []string, rawCols []interface{}) []string {
	resultCols := make([]string, len(cols))
	for i, _ := range cols {
		val := rawCols[i]
		if val == nil {
			resultCols[i] = ""
		} else {
			resultCols[i] = switchType(val)
		}
	}
	return resultCols
}

//RowtoMap conver to  []map[string]string
func RowToMap(rows *sql.Rows) []map[string]string {
	columns, _ := rows.Columns()
	count := len(columns)
	readCols := make([]interface{}, count)
	rawCols := make([]interface{}, count)
	var records []map[string]string
	for rows.Next() {
		// resultCols := make(map[string]string, count)
		for i := range columns {
			readCols[i] = &rawCols[i]
		}
		rows.Scan(readCols...)

		// all conver to string
		resultCols := assertTypeMap(columns, rawCols)

		records = append(records, resultCols)
	}
	return records
}

//RowToArr convert sql.Rows to array, first row is col names
func RowToArr(rows *sql.Rows) (records [][]string, err error) {
	fmt.Printf("RowToArr start at %s", time.Now())
	columns, err := rows.Columns()
	if err != nil {
		return
	}
	count := len(columns)
	readCols := make([]interface{}, count)
	rawCols := make([]interface{}, count)
	//records = make([]interface{}, 0)
	records = append(records, columns) //append row header as 1st row

	// var resultCols []string
	for rows.Next() {
		// resultCols = make([]string, count)
		for i := range columns {
			readCols[i] = &rawCols[i]
		}
		err = rows.Scan(readCols...)
		if err != nil {
			return
		}

		resultCols := assertTypeArray(columns, rawCols)
		records = append(records, resultCols)
	}

	fmt.Printf("RowToArr end at %s", time.Now())
	return records, nil
}

//RowToRawData conver sql Rows to RawData with Rows: map[string]string, Headers: []string
func RowToRawData(rows *sql.Rows) (r RawData) {
	record, _ := RowToArr(rows)
	r.Header = record[0]
	r.Rows = append(r.Rows, record[1:])
	return
}

//SingleRowToArrayChan
func RowToArrayChan(rows *sql.Rows) chan interface{} {
	resultC := make(chan interface{})

	columns, err := rows.Columns()
	//for mongodb storage, replace . with _ in columns

	if err != nil {
		resultC <- err
		close(resultC)
		return resultC
	}

	count := len(columns)
	readCols := make([]interface{}, count)
	rawCols := make([]interface{}, count)
	//records = make([]interface{}, 0)
	go func() {
		for rows.Next() {
			// resultCols := make([]string, count)
			for i := range columns {
				readCols[i] = &rawCols[i]
			}
			err = rows.Scan(readCols...)
			if err != nil {
				resultC <- err
				break
			}

			// all conver to string
			resultCols := assertTypeArray(columns, rawCols)

			resultC <- resultCols
		}
		close(resultC)
	}()
	return resultC
}
