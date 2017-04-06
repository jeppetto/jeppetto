/*
 * Copyright (c) 2011-2017 Jeppetto and Jonathan Thompson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.iternine.jeppetto.dao.dynamodb.extra.indexes;


import org.iternine.jeppetto.dao.AccessControlContextProvider;
import org.iternine.jeppetto.dao.DAOBuilder;
import org.iternine.jeppetto.dao.dynamodb.DynamoDBQueryModelDAO;
import org.iternine.jeppetto.dao.dynamodb.iterable.DynamoDBIterable;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import junit.framework.Assert;
import org.junit.After;
import org.junit.BeforeClass;

import java.util.HashMap;
import java.util.Map;


public class BaseDynamoDBIndexTest {

    //-------------------------------------------------------------
    // Variables - Static
    //-------------------------------------------------------------

    protected static AmazonDynamoDB amazonDynamoDB;


    //-------------------------------------------------------------
    // Methods - Test Lifecycle
    //-------------------------------------------------------------

    @BeforeClass
    public static void beforeClass() {
        amazonDynamoDB = new AmazonDynamoDBClient(new BasicAWSCredentials("", ""));
        amazonDynamoDB.setEndpoint(String.format("http://localhost:%s", System.getProperty("dynamodb.port")));
    }


    @After
    public void after() {
        if (amazonDynamoDB == null) {
            return;
        }

        ListTablesResult listTablesResult = amazonDynamoDB.listTables();

        for (String tableName : listTablesResult.getTableNames()) {
            amazonDynamoDB.deleteTable(tableName);
        }
    }


    //-------------------------------------------------------------
    // Methods - Protected
    //-------------------------------------------------------------

    protected ItemDAO getItemDAO() {
        return DAOBuilder.buildDAO(Item.class, ItemDAO.class, ItemDynamoDBQueryModel.class, new HashMap<String, Object>() {{
            put("db", amazonDynamoDB);
        }});
    }


    protected String getPage(DynamoDBIterable<?> iterable, int pageSize, String queryPosition, int expected) {
        iterable.setPosition(queryPosition);
        iterable.setLimit(pageSize);

        int count = 0;
        for (Object ignored : iterable) {
            count++;
        }

        Assert.assertEquals(expected, count);

        return iterable.hasResultsPastLimit() ? iterable.getPosition() : null;
    }


    //-------------------------------------------------------------
    // Inner Class - ItemDynamoDBQueryModel
    //-------------------------------------------------------------

    @SuppressWarnings("unused")
    protected static class ItemDynamoDBQueryModel extends DynamoDBQueryModelDAO<Item, String> {

        protected ItemDynamoDBQueryModel(Class<Item> entityClass, Map<String, Object> daoProperties) {
            super(entityClass, daoProperties);
        }

        protected ItemDynamoDBQueryModel(Class<Item> entityClass, Map<String, Object> daoProperties,
                                         AccessControlContextProvider accessControlContextProvider) {
            super(entityClass, daoProperties, accessControlContextProvider);
        }
    }
}
