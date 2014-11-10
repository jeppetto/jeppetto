package org.iternine.jeppetto.testsupport;


import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;

import org.iternine.jeppetto.testsupport.db.Database;
import org.iternine.jeppetto.testsupport.db.DynamoDBDatabase;
import org.springframework.context.ApplicationContext;

import java.util.List;
import java.util.Map;
import java.util.Properties;


public class DynamoDBDatabaseProvider
        implements DatabaseProvider {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private List<CreateTableRequest> createTableRequests;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public DynamoDBDatabaseProvider() {
        this(null);
    }


    public DynamoDBDatabaseProvider(List<CreateTableRequest> createTableRequests) {
        this.createTableRequests = createTableRequests;
    }


    //-------------------------------------------------------------
    // Implementation - DatabaseProvider
    //-------------------------------------------------------------

    @Override
    public Properties modifyProperties(Properties properties) {
        properties.put("dynamodb.endpoint", String.format("http://localhost:%s", System.getProperty("dynamodb.port")));

        return properties;
    }


    @Override
    public Database getDatabase(Properties properties, ApplicationContext applicationContext) {
        Map<String, AmazonDynamoDB> amazonDynamoDBBeans = applicationContext.getBeansOfType(AmazonDynamoDB.class);

        if (amazonDynamoDBBeans.size() != 1) {
            throw new RuntimeException("Expected one 'AmazonDynamoDB' definition.  Found " + amazonDynamoDBBeans.size());
        }

        return new DynamoDBDatabase((AmazonDynamoDB) amazonDynamoDBBeans.values().toArray()[0], createTableRequests);
    }
}
