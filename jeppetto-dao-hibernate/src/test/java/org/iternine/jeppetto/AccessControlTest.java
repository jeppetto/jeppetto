/*
 * Copyright (c) 2011 Jeppetto and Jonathan Thompson
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

package org.iternine.jeppetto;


import org.iternine.jeppetto.dao.NoSuchItemException;
import org.iternine.jeppetto.dao.SimpleAccessControlContext;
import org.iternine.jeppetto.testsupport.TestContext;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;


public class AccessControlTest {

    //-------------------------------------------------------------
    // Variables - Private - Static
    //-------------------------------------------------------------

    private TestContext testContext;
    private AccessControllableSimpleObjectDAO simpleObjectDAO;
    private SettableAccessControlContextProvider accessControlContextProvider;
    private SimpleAccessControlContext accessControlContext1;
    private SimpleAccessControlContext accessControlContext2;
    private SimpleAccessControlContext accessControlContext3;


    //-------------------------------------------------------------
    // Methods - Set-Up / Tear-Down
    //-------------------------------------------------------------

    @Before
    public void setUp() {
        testContext = new TestContext("AccessControlTest.spring.xml",
                                      "HibernateDAOTest.test.properties",
                                      "hibernateDAOTest.jdbc.driverClass");

        simpleObjectDAO = (AccessControllableSimpleObjectDAO) testContext.getBean("transactionalAccessControllableSimpleObjectDAO");
        accessControlContextProvider = (SettableAccessControlContextProvider) simpleObjectDAO.getAccessControlContextProvider();

        accessControlContext1 = new SimpleAccessControlContext();
        accessControlContext1.setAccessId("001");
        accessControlContext1.setRole("User");

        accessControlContext2 = new SimpleAccessControlContext();
        accessControlContext2.setAccessId("002");
        accessControlContext2.setRole("User");

        accessControlContext3 = new SimpleAccessControlContext();
        accessControlContext3.setAccessId("003");
        accessControlContext3.setRole("Administrator");
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
    public void authorizedAccessAttempt()
            throws NoSuchItemException {
        accessControlContextProvider.setCurrent(accessControlContext1);

        SimpleObject simpleObject = new SimpleObject();

        simpleObjectDAO.save(simpleObject);

        SimpleObject resultObject = simpleObjectDAO.findById(simpleObject.getId());

        Assert.assertEquals(resultObject.getId(), simpleObject.getId());
    }


    @Test(expected = NoSuchItemException.class)
    public void unauthorizedAccessAttempt()
            throws NoSuchItemException {
        accessControlContextProvider.setCurrent(accessControlContext1);

        SimpleObject simpleObject = new SimpleObject();

        simpleObjectDAO.save(simpleObject);

        accessControlContextProvider.setCurrent(accessControlContext2);

        testContext.getDatabase().writeXmlDataSet(System.out);

        simpleObjectDAO.findById(simpleObject.getId());
    }


    @Test
    public void getList() {
        accessControlContextProvider.setCurrent(accessControlContext1);

        for (int i = 0; i < 10; i++) {
            simpleObjectDAO.save(new SimpleObject());
        }

        accessControlContextProvider.setCurrent(accessControlContext2);

        for (int i = 0; i < 5; i++) {
            simpleObjectDAO.save(new SimpleObject());
        }

        accessControlContextProvider.setCurrent(accessControlContext1);

        Iterable<SimpleObject> simpleObjectsAvailableToUser1 = simpleObjectDAO.findAll();

        String randomSimpleObjectId = null;
        int count = 0;

        for (SimpleObject simpleObject : simpleObjectsAvailableToUser1) {
            if (randomSimpleObjectId == null) {
                randomSimpleObjectId = simpleObject.getId();
            }
            count++;
        }

        Assert.assertEquals(10, count);

        accessControlContextProvider.setCurrent(accessControlContext2);

        Iterable<SimpleObject> simpleObjectsAvailableToUser2 = simpleObjectDAO.findAll();

        int count2 = 0;
        //noinspection UnusedDeclaration
        for (SimpleObject simpleObject : simpleObjectsAvailableToUser2) {
            count2++;
        }

        Assert.assertEquals(5, count2);
        for (SimpleObject simpleObject : simpleObjectsAvailableToUser2) {
            Assert.assertNotSame(randomSimpleObjectId, simpleObject.getId());
        }
    }


    @Test
    public void allowedRoleAccessAttempt()
            throws NoSuchItemException {
        accessControlContextProvider.setCurrent(accessControlContext1);

        SimpleObject simpleObject = new SimpleObject();

        simpleObjectDAO.save(simpleObject);

        accessControlContextProvider.setCurrent(accessControlContext3);

        SimpleObject resultObject = simpleObjectDAO.findById(simpleObject.getId());

        Assert.assertEquals(resultObject.getId(), simpleObject.getId());
    }


    @Test
    public void verifyOrderByWorks() {
        accessControlContextProvider.setCurrent(accessControlContext1);

        for (int i = 0; i < 10; i++) {
            simpleObjectDAO.save(new SimpleObject());
        }

        List<SimpleObject> orderedItems = simpleObjectDAO.findByOrderById();

        Assert.assertEquals(10, orderedItems.size());

        String lastId = null;
        for (SimpleObject orderedItem : orderedItems) {
            if (lastId != null) {
                Assert.assertTrue("lastId is not less than thisId: " + lastId + " !< " + orderedItem.getId(),
                                  lastId.compareTo(orderedItem.getId()) < 0);
            }

            lastId = orderedItem.getId();
        }
    }


    @Test
    public void associationAccessAttempt()
            throws NoSuchItemException {
        accessControlContextProvider.setCurrent(accessControlContext1);

        SimpleObject simpleObject = new SimpleObject();

        RelatedObject relatedObject = new RelatedObject();
        relatedObject.setRelatedIntValue(345);

        simpleObject.setRelatedObjects(Collections.singleton(relatedObject));

        simpleObjectDAO.save(simpleObject);

        simpleObject = new SimpleObject();

        simpleObjectDAO.save(simpleObject);

        List<SimpleObject> resultObjects = simpleObjectDAO.findByHavingRelatedObjectsWithRelatedIntValue(345);

        Assert.assertEquals(1, resultObjects.size());
    }


    @Test
    public void checkAnnotationQueryWorks() {
        accessControlContextProvider.setCurrent(accessControlContext1);

        for (int i = 1; i < 10; i++) {
            simpleObjectDAO.save(new SimpleObject(i));
        }

        accessControlContextProvider.setCurrent(accessControlContext2);

        for (int i = 2; i < 10; i++) {
            simpleObjectDAO.save(new SimpleObject(i));
        }

        accessControlContextProvider.setCurrent(accessControlContext1);

        Assert.assertEquals(3, simpleObjectDAO.getByIntValueLessThan(4).size());
        Assert.assertEquals(2, simpleObjectDAO.getByIntValueLessThanSpecifyingContext(4, accessControlContext2).size());
        Assert.assertEquals(5, simpleObjectDAO.getByIntValueLessThanUsingAdministratorRole(4).size());
        Assert.assertEquals(0, simpleObjectDAO.getByIntValueLessThanUsingBogusRole(4).size());
    }
}
