/*
 * Copyright (c) 2011-2014 Jeppetto and Jonathan Thompson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.iternine.jeppetto.testsupport.db;


import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;

import org.dbunit.database.IDatabaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class DynamoDBDatabase extends Database {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private AmazonDynamoDB amazonDynamoDB;

    private static Logger logger = LoggerFactory.getLogger(DynamoDBDatabase.class);


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public DynamoDBDatabase(AmazonDynamoDB amazonDynamoDB, List<CreateTableRequest> createTableRequests) {
        super(null);

        this.amazonDynamoDB = amazonDynamoDB;

        if (createTableRequests != null) {
            createTables(createTableRequests);
        }
    }


    //-------------------------------------------------------------
    // Methods - Implementation
    //-------------------------------------------------------------

    @Override
    public void close() {
        ListTablesResult listTablesResult = amazonDynamoDB.listTables();

        for (String tableName : listTablesResult.getTableNames()) {
            amazonDynamoDB.deleteTable(tableName);
        }
    }


    @Override
    protected void onNewIDatabaseConnection(IDatabaseConnection connection) {
        throw new UnsupportedOperationException("DynamoDBDatabase does not support new IDatabaseConnections.");
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private void createTables(List<CreateTableRequest> createTableRequests) {
        for (CreateTableRequest createTableRequest : createTableRequests) {
            CreateTableResult result = amazonDynamoDB.createTable(createTableRequest);

            logger.debug("Created table: " + result.getTableDescription());
        }
    }
}
