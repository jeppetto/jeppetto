/*
 * Copyright (c) 2011 Jeppetto and Jonathan Thompson
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

package org.jeppetto.dao.mongodb;


import org.jeppetto.dao.NoSuchItemException;
import org.jeppetto.testsupport.MongoDatabaseProvider;
import org.jeppetto.testsupport.TestContext;

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
    private SimpleObjectDAO simpleObjectDAO;


    //-------------------------------------------------------------
    // Methods - Set-Up / Tear-Down
    //-------------------------------------------------------------

    @Before
    public void setUp() {
        testContext = new TestContext("MongoDAOTest.spring.xml",
                                      "MongoDAOTest.properties",
                                      new MongoDatabaseProvider(getClass().getSimpleName()));

        simpleObjectDAO = (SimpleObjectDAO) testContext.getBean("simpleObjectDAO");
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

            simpleObjectDAO.save(simpleObject);

            Object obj1 = simpleObjectDAO.findById(simpleObject.getId());
            Object obj2 = simpleObjectDAO.findById(simpleObject.getId());
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

            simpleObjectDAO.save(simpleObject);

            SimpleObject fromCache1 = simpleObjectDAO.findByIntValue(1234);
            SimpleObject fromCache2 = simpleObjectDAO.findById(simpleObject.getId());

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

        simpleObjectDAO.save(simpleObject);

        MongoDBSession.flush();

        SimpleObject resultObject = simpleObjectDAO.findById(simpleObject.getId());

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
            simpleObjectDAO.save(obj);
        }
        MongoDBSession.flush();
        for (int i = 0; i < n; i++) {
            MongoDBSession.remove();
        }
    }
}
