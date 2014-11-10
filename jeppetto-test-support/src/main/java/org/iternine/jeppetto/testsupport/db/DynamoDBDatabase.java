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
