package sqlutils

import (
	"database/sql"
	"fmt"
	"testing"

	// _ "gopkg.in/goracle.v1"
	_ "gopkg.in/rana/ora.v3"
)

var (
	oracle_tns    string = "xe"
	oracle_user   string = "system"
	oracle_passwd string = "oracle"
	sql_body      string = "select 1+2 from dual"

	DB       string
	OracleDB *sql.DB
	Rows     *sql.Rows
)

func prefunc() {
	connect_str := fmt.Sprintf("%s/%s@%s", oracle_user, oracle_passwd, oracle_tns)
	OracleDB, err := sql.Open("ora", connect_str)
	defer OracleDB.Close()
	if err != nil {
		fmt.Println("%v", err)
	}
	Rows, err = OracleDB.Query(sql_body)
	if err != nil {
		fmt.Println("%v", err)
	}

	// fmt.Println(Rows.Columns)
}

func TestRowToArr(t *testing.T) {
	prefunc()
	v := RowToArr(Rows)
	if v[0][0] != "1+2" {
		t.Error("Expected [1+2][3], got: ", v)
	}
}

func TestRowToRawData(t *testing.T) {
	prefunc()
	v := RowToRawData(Rows)
	if v.Header[0] != "1+2" {
		t.Error("Expected Header 1+2, got :", v)
	}
}

func TestRowToCSV(t *testing.T) {
	prefunc()
	v := RowToCSV(Rows)
	if v[0:3] != "1+2" {
		t.Error("Expected CSV line  1+2, got :", v)
	}
}

func TestRowToArrayJSON(t *testing.T) {
	prefunc()
	v := RowToArrayJSON(Rows)
	output := `{"Header":["1+2"],"Rows":[[["3"]]]}`
	if v != output {
		t.Error(`Expected Arry JSON {"Header":["1+2"],"Rows":[[["3"]]]}  got :`, v)
	}
}

func TestRowToMap(t *testing.T) {
	prefunc()
	v := RowToMap(Rows)
	if v[0]["1+2"] != "3" {
		t.Error(`Expected row 0 with index  "1+2" = 3  got :`, v)
	}

}

func TestRowToMapJSON(t *testing.T) {
	prefunc()
	v := RowToMapJSON(Rows)
	if v != `[{"1+2":"3"}]` {
		t.Error(`Expected row 0 with index  "1+2" = 3  got :`, v)
	}
}
