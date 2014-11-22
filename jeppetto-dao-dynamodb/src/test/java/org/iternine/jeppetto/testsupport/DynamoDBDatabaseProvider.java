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
