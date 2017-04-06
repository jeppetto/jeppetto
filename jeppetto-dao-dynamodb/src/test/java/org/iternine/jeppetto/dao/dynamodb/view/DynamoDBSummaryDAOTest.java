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

package org.iternine.jeppetto.dao.dynamodb.view;


import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

import org.iternine.jeppetto.dao.test.core.DynamicDAO;
import org.iternine.jeppetto.dao.test.view.SummaryDAO;
import org.iternine.jeppetto.dao.test.view.SummaryDAOTest;
import org.iternine.jeppetto.testsupport.DynamoDBDatabaseProvider;
import org.iternine.jeppetto.testsupport.TestContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class DynamoDBSummaryDAOTest extends SummaryDAOTest {

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
    // Implementation - DynamicDAOTest
    //-------------------------------------------------------------

    @Override
    protected DynamicDAO getDynamicDAO() {
        if (testContext == null) {
            testContext = new TestContext("DynamoDBSummaryDAOTest.spring.xml",
                                          "DynamoDBTest.properties",
                                          new DynamoDBDatabaseProvider(createTableRequests));
        }

        //noinspection unchecked
        return (DynamicDAO) testContext.getBean("dynamoDBDynamicDAO");
    }


    @Override
    protected SummaryDAO getSummaryDAO() {
        if (testContext == null) {
            testContext = new TestContext("DynamoDBSummaryDAOTest.spring.xml",
                                          "DynamoDBTest.properties",
                                          new DynamoDBDatabaseProvider(createTableRequests));
        }

        return (SummaryDAO) testContext.getBean("dynamoDBSummaryDAO");
    }


    @Override
    protected void reset() {
        if (testContext != null) {
            testContext.close();

            testContext = null;
        }
    }
}
