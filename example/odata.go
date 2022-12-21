/* Apache v2 license
*  Copyright (C) <2019> Intel Corporation
*
*  SPDX-License-Identifier: Apache-2.0
 */

package example

import (
	"context"
	"fmt"
	"net/url"

	odata "github.com/9spokes/odata/mongo"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func example() {

	var dbhost = "mongodb://localhost:27017/testdb"

	mainSession, err := mongo.Connect(context.Background(), options.Client().ApplyURI(dbhost))
	if err != nil {
		fmt.Errorf("Unable to connect to mongo server on %s", dbhost)
	}

	defer mainSession.Disconnect(context.Background())

	testURL, err := url.Parse("http://127.0.0.1/test?$top=10&$select=name,age&$orderby=time asc,name desc,age")
	if err != nil {
		fmt.Errorf("failed to parse test url")
	}

	var object []interface{}
	collection := mainSession.Database("testdb").Collection("collectionName")

	if _, err := odata.ODataQuery("", testURL.Query(), &object, collection); err != nil {
		fmt.Errorf("Error: %s", err.Error())
	}
}
