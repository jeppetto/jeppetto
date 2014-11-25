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

package org.iternine.jeppetto.dao.dynamodb.updateobject;


import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

import org.iternine.jeppetto.dao.JeppettoException;
import org.iternine.jeppetto.dao.test.updateobject.UpdateObjectDAO;
import org.iternine.jeppetto.dao.test.updateobject.UpdateObjectDAOTest;
import org.iternine.jeppetto.testsupport.DynamoDBDatabaseProvider;
import org.iternine.jeppetto.testsupport.TestContext;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class DynamoDBUpdateObjectDAOTest extends UpdateObjectDAOTest {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private TestContext testContext;
    private List<CreateTableRequest> createTableRequests = new ArrayList<CreateTableRequest>() { {
        add(new CreateTableRequest(Collections.singletonList(new AttributeDefinition("id", ScalarAttributeType.S)),
                                   "SimpleObject",
                                   Collections.singletonList(new KeySchemaElement("id", KeyType.HASH)),
                                   new ProvisionedThroughput(1L, 1L)));
    } };


    //-------------------------------------------------------------
    // Implementation - SimpleObjectDAOTest
    //-------------------------------------------------------------

    @Override
    protected UpdateObjectDAO getSimpleObjectReferencesDAO() {
        if (testContext == null) {
            testContext = new TestContext("DynamoDBUpdateObjectDAOTest.spring.xml",
                                          "DynamoDBTest.properties",
                                          new DynamoDBDatabaseProvider(createTableRequests));
        }

        //noinspection unchecked
        return (UpdateObjectDAO) testContext.getBean("dynamoDBUpdateObjectDAO");
    }


    @Override
    protected void reset() {
        if (testContext != null) {
            testContext.close();

            testContext = null;
        }
    }


    //-------------------------------------------------------------
    // Tests
    //-------------------------------------------------------------

    @Ignore("DynamoDB doesn't support removing an item by value, only by path.")
    @Test
    public void removeFromExistingList() {
    }


    @Ignore("DynamoDB doesn't support updates when the key isn't part of the query phrase.")
    @Test(expected = JeppettoException.class)
    public void updateByOtherField() {
    }


    @Ignore("UpdateSet not yet implemented.")
    @Test
    public void addStringsToSet() {
    }
}
