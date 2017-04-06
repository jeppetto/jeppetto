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

package org.iternine.jeppetto.dao.mongodb;


import org.iternine.jeppetto.dao.NoSuchItemException;
import org.iternine.jeppetto.dao.test.SimpleObject;
import org.iternine.jeppetto.dao.test.core.DynamicDAO;
import org.iternine.jeppetto.testsupport.MongoDatabaseProvider;
import org.iternine.jeppetto.testsupport.TestContext;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;


public class MongoSessionTest {

    //-------------------------------------------------------------
    // Variables - Private - Static
    //-------------------------------------------------------------

    private TestContext testContext;
    private DynamicDAO dynamicDAO;


    //-------------------------------------------------------------
    // Methods - Set-Up / Tear-Down
    //-------------------------------------------------------------

    @Before
    public void setUp() {
        testContext = new TestContext("MongoDAOTest.spring.xml",
                                      "MongoDAOTest.properties",
                                      new MongoDatabaseProvider());

        dynamicDAO = (DynamicDAO) testContext.getBean("mongoDynamicDAO");
    }


    @After
    public void tearDown() {
        if (testContext != null) {
            testContext.close();
        }
    }


    //-------------------------------------------------------------
    // Methods - Test Cases
    //-------------------------------------------------------------

    @Test
    public void findWithoutFlushDoesNotFail()
            throws NoSuchItemException {
        try {
            MongoDBSession.create();

            SimpleObject simpleObject = new SimpleObject();
            simpleObject.setIntValue(1234);

            dynamicDAO.save(simpleObject);

            Object obj1 = dynamicDAO.findById(simpleObject.getId());
            Object obj2 = dynamicDAO.findById(simpleObject.getId());
            assertSame(obj1, obj2);
        } finally {
            MongoDBSession.remove();
        }
    }


    @Test
    public void saveThenFindByUniquenessConstraintWorksWithSession()
            throws NoSuchItemException {

        try {
            MongoDBSession.create();

            SimpleObject simpleObject = new SimpleObject();
            simpleObject.setIntValue(1234);

            dynamicDAO.save(simpleObject);

            SimpleObject fromCache1 = dynamicDAO.findByIntValue(1234);
            SimpleObject fromCache2 = dynamicDAO.findById(simpleObject.getId());

            assertNotNull(fromCache1);
            assertNotNull(fromCache2);

            assertEquals(simpleObject.getId(), fromCache1.getId());
            assertEquals(simpleObject.getId(), fromCache2.getId());
            assertSame(fromCache1, fromCache2);
        } finally {
            MongoDBSession.remove();
        }
    }


    @Test
    public void saveFlushAndFind()
            throws NoSuchItemException {
        MongoDBSession.create();

        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(1234);

        dynamicDAO.save(simpleObject);

        MongoDBSession.flush();

        SimpleObject resultObject = dynamicDAO.findById(simpleObject.getId());

        assertEquals(resultObject.getId(), simpleObject.getId());

        MongoDBSession.remove();
    }


    @Test
    public void sessionIsReentrant() {
        int n = 1000;
        for (int i = 0; i < n; i++) {
            MongoDBSession.create();
            SimpleObject obj = new SimpleObject();
            obj.setIntValue(-i);
            dynamicDAO.save(obj);
        }
        MongoDBSession.flush();
        for (int i = 0; i < n; i++) {
            MongoDBSession.remove();
        }
    }
}
