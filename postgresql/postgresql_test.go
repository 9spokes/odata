package postgresql

import (
	"fmt"
	"net/url"
	"os"
	"testing"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" // postgreSQL driver
	"github.com/pkg/errors"
)

// For testing, run PostgreSQL as a docker container
// docker run -p 5432:5432 postgres:11-alpine
// existing SQL mock testing libraries do not support jsonb

func TestFilter(t *testing.T) {

	db := dbSetup()

	var filterTests = []struct {
		input    string
		expected bool
		Err      error
	}{
		{"((epc_item_type gt 0) and (event ne 'departed') and (required eq true) and (count lt 0.1) or " +
			"(SKU eq '123') and contains(epc_time, '0') and startswith(epc_code, '456') or " +
			"endswith(upc_code, '789'))", true, nil}, // large valid filter case
		{"name eq 'test'", true, nil},                                      // key name with operator
		{"age gt 10", true, nil},                                           // key name with operator
		{"age ge 10", true, nil},                                           // key name with operator
		{"age lt 10", true, nil},                                           // key name with operator
		{"age le 10", true, nil},                                           // key name with operator
		{"(name eq 'test') and (age gt 10) or (age le 10)", true, nil},     // key name with operator
		{"name eq 'val')", false, errors.New("")},                          // bad parentheses
		{"(name eq )", false, errors.New("")},                              // missing value in equals operator
		{"(name eq hello) and (name fakeop hello)", false, errors.New("")}, // bad operator
		{"epc_item_type ne 0 and name", false, errors.New("")},             // operators can't have a mix operators and literals
		{"epc_item_type ne 0 and 0", false, errors.New("")},                // // operators can't have a mix operators and literals
		{"name and epc_item_type ne 0", false, errors.New("")},             // // operators can't have a mix operators and literals
		{"name eqs epc_item_type", false, errors.New("")},                  // typo operator
		{"contains(and, epc_item_type)", false, errors.New("")},            // operator in function
		{"0 eq epc_item_type", false, errors.New("")},                      // integer key name
		{"", false, errors.New("")},                                        // empty string test
	}

	for _, expectedVal := range filterTests {
		var testURLString = fmt.Sprintf("http://localhost/test?$filter=%s", expectedVal.input)
		testURL, err := url.Parse(testURLString)
		if err != nil {
			t.Fatalf("Failed to parse test url")
			return
		}

		_, errorQuery := ODataSQLQuery(testURL.Query(), "test", "data", db)

		if (errorQuery != nil) != (expectedVal.Err != nil) {
			t.Errorf("Expected error mismatch. Error = %s", errorQuery)
		}

	}

}

func TestODataQuery(t *testing.T) {

	db := dbSetup()

	testURL, err := url.Parse("http://localhost/test?$top=10&$skip=10&$select=name,lastname,age&$orderby=time asc,name desc,age")
	if err != nil {
		t.Error("failed to parse test url")
	}

	_, errorQuery := ODataSQLQuery(testURL.Query(), "test", "data", db)

	if errorQuery != nil {
		t.Error(errorQuery)
	}

}

func TestCount(t *testing.T) {

	db := dbSetup()

	_, errorQuery := ODataCount(db, "test")

	if errorQuery != nil {
		t.Error(errorQuery)
	}

}

func dbSetup() *sqlx.DB {

	const schema = `
			CREATE TABLE IF NOT EXISTS test (	
				data JSONB	
			);
	`

	q := make(url.Values)
	q.Set("sslmode", "disable")

	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword("postgres", ""),
		Host:     "localhost",
		Path:     "postgres",
		RawQuery: q.Encode(),
	}

	// Connect to PostgreSQL
	db, err := sqlx.Open("postgres", u.String())
	if err != nil {
		os.Exit(1)
	}
	// Create table
	db.MustExec(schema)

	return db
}
