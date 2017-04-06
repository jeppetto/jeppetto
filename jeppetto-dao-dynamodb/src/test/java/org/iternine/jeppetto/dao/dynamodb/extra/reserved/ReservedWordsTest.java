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

package org.iternine.jeppetto.dao.dynamodb.extra.reserved;


import org.iternine.jeppetto.dao.AccessControlContextProvider;
import org.iternine.jeppetto.dao.DAOBuilder;
import org.iternine.jeppetto.dao.dynamodb.DynamoDBQueryModelDAO;
import org.iternine.jeppetto.dao.dynamodb.extra.TableBuilder;
import org.iternine.jeppetto.dao.id.BaseNIdGenerator;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;


public class ReservedWordsTest {

    //-------------------------------------------------------------
    // Variables - Private - Static
    //-------------------------------------------------------------

    private static L0DAO l0Dao;


    //-------------------------------------------------------------
    // Methods - Test Lifecycle
    //-------------------------------------------------------------

    @BeforeClass
    public static void beforeClass() {
        final AmazonDynamoDB amazonDynamoDB = new AmazonDynamoDBClient(new BasicAWSCredentials("", ""));
        amazonDynamoDB.setEndpoint(String.format("http://localhost:%s", System.getProperty("dynamodb.port")));

        new TableBuilder("TableL0").withKey("id").build(amazonDynamoDB);

        l0Dao = DAOBuilder.buildDAO(L0.class, L0DAO.class, L0DynamoDBQueryModel.class, new HashMap<String, Object>() {{
            put("db", amazonDynamoDB);
            put("tableName", "TableL0");
            put("idGenerator", new BaseNIdGenerator(31, BaseNIdGenerator.BASE36_CHARACTERS));
            put("enableScans", "true");
        }});
    }


    @After
    public void after() {
        for (L0 l0 : l0Dao.findAll()) {
            l0Dao.delete(l0);
        }
    }


    @AfterClass
    public static void afterClass() {
        final AmazonDynamoDB amazonDynamoDB = new AmazonDynamoDBClient(new BasicAWSCredentials("", ""));
        amazonDynamoDB.setEndpoint(String.format("http://localhost:%s", System.getProperty("dynamodb.port")));

        ListTablesResult listTablesResult = amazonDynamoDB.listTables();

        for (String tableName : listTablesResult.getTableNames()) {
            amazonDynamoDB.deleteTable(tableName);
        }
    }


    //-------------------------------------------------------------
    // Methods - Tests
    //-------------------------------------------------------------

    @Test
    public void test() {
        L0 l0 = new L0("foo", new HashMap<String, L1>() {{
            put("l1.map", new L1("bar", new HashMap<String, L2>() {{
                put("l2.map", new L2("bar", new HashMap<String, L3>() {{
                     put("l3.map", new L3());
                }}));
            }}));
        }});

        l0Dao.save(l0);

        L0 readL0 = l0Dao.findById(l0.getId());

        readL0.getL1Map().get("l1.map").getL2Map().get("l2.map").setNext("baz");
        // update readL0;

        l0Dao.save(readL0);

        L0 updatedL0 = l0Dao.findById(l0.getId());

        Assert.assertEquals("baz", updatedL0.getL1Map().get("l1.map").getL2Map().get("l2.map").getNext());
    }


    //-------------------------------------------------------------
    // Inner Class - L0DynamoDBQueryModel
    //-------------------------------------------------------------

    @SuppressWarnings("unused")
    protected static class L0DynamoDBQueryModel extends DynamoDBQueryModelDAO<L0, String> {

        protected L0DynamoDBQueryModel(Class<L0> entityClass, Map<String, Object> daoProperties) {
            super(entityClass, daoProperties);
        }

        protected L0DynamoDBQueryModel(Class<L0> entityClass, Map<String, Object> daoProperties,
                                       AccessControlContextProvider accessControlContextProvider) {
            super(entityClass, daoProperties, accessControlContextProvider);
        }
    }
}
