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

package org.iternine.jeppetto.dao.dynamodb.core;


import org.iternine.jeppetto.dao.GenericDAO;
import org.iternine.jeppetto.dao.NoSuchItemException;
import org.iternine.jeppetto.dao.test.RelatedObject;
import org.iternine.jeppetto.dao.test.SimpleObject;
import org.iternine.jeppetto.dao.test.core.GenericDAOTest;
import org.iternine.jeppetto.testsupport.DynamoDBDatabaseProvider;
import org.iternine.jeppetto.testsupport.TestContext;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;

import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;


public class DynamoDBGenericDAOTest extends GenericDAOTest {

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
    // Implementation - GenericDAOTest
    //-------------------------------------------------------------

    @Override
    protected GenericDAO<SimpleObject, String> getGenericDAO() {
        if (testContext == null) {
            testContext = new TestContext("DynamoDBGenericDAOTest.spring.xml",
                                          "DynamoDBTest.properties",
                                          new DynamoDBDatabaseProvider(createTableRequests));
        }

        //noinspection unchecked
        return (GenericDAO<SimpleObject, String>) testContext.getBean("dynamoDBGenericDAO");
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

    @Ignore
    @Test
    public void uniqueConstraintCausesException() {
    }


    @Test
    public void saveAndRetrieveLargeList()
            throws NoSuchItemException {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(1234);

        for (int i = 0; i < 120; i++) {
            RelatedObject relatedObject = new RelatedObject();
            relatedObject.setRelatedIntValue(i);

            simpleObject.addRelatedObject(relatedObject);
        }

        getGenericDAO().save(simpleObject);

        SimpleObject resultObject = getGenericDAO().findById(simpleObject.getId());

        assertEquals(resultObject.getId(), simpleObject.getId());
        assertEquals(120, resultObject.getRelatedObjects().size());
    }
}
