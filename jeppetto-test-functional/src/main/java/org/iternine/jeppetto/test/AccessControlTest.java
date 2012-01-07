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

package org.iternine.jeppetto.test;


import org.iternine.jeppetto.dao.NoSuchItemException;
import org.iternine.jeppetto.dao.SimpleAccessControlContext;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;


public abstract class AccessControlTest {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private SettableAccessControlContextProvider accessControlContextProvider;

    private static SimpleAccessControlContext accessControlContext1;
    private static SimpleAccessControlContext accessControlContext2;
    private static SimpleAccessControlContext accessControlContext3;
    private static SimpleAccessControlContext accessControlContext4;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    static {
        accessControlContext1 = new SimpleAccessControlContext();
        accessControlContext1.setAccessId("001");
        accessControlContext1.setRole("User");

        accessControlContext2 = new SimpleAccessControlContext();
        accessControlContext2.setAccessId("002");
        accessControlContext2.setRole("User");

        accessControlContext3 = new SimpleAccessControlContext();
        accessControlContext3.setAccessId("003");
        accessControlContext3.setRole("Administrator");

        accessControlContext4 = new SimpleAccessControlContext();   // No accessId or role
    }


    //-------------------------------------------------------------
    // Methods - Abstract
    //-------------------------------------------------------------

    protected abstract AccessControlTestDAO getAccessControlTestDAO();

    protected abstract void reset();


    //-------------------------------------------------------------
    // Methods - Test Lifecycle
    //-------------------------------------------------------------

    @Before
    public void before() {
        accessControlContextProvider = (SettableAccessControlContextProvider) getAccessControlTestDAO().getAccessControlContextProvider();
    }


    @After
    public void after() {
        reset();

        accessControlContextProvider = null;
    }


    //-------------------------------------------------------------
    // Methods - Test Cases
    //-------------------------------------------------------------

    @Test
    public void authorizedAccessAttempt()
            throws NoSuchItemException {
        accessControlContextProvider.setCurrent(accessControlContext1);

        SimpleObject simpleObject = new SimpleObject();

        getAccessControlTestDAO().save(simpleObject);

        SimpleObject resultObject = getAccessControlTestDAO().findById(simpleObject.getId());

        Assert.assertEquals(resultObject.getId(), simpleObject.getId());
    }


    @Test(expected = NoSuchItemException.class)
    public void unauthorizedAccessAttempt()
            throws NoSuchItemException {
        accessControlContextProvider.setCurrent(accessControlContext1);

        SimpleObject simpleObject = new SimpleObject();

        getAccessControlTestDAO().save(simpleObject);

        accessControlContextProvider.setCurrent(accessControlContext2);

        getAccessControlTestDAO().findById(simpleObject.getId());
    }


    @Test(expected = NoSuchItemException.class)
    public void unauthorizedAccessAttemptWithEmptyAccessControlContext()
            throws NoSuchItemException {
        accessControlContextProvider.setCurrent(accessControlContext1);

        SimpleObject simpleObject = new SimpleObject();

        getAccessControlTestDAO().save(simpleObject);

        accessControlContextProvider.setCurrent(accessControlContext4);

        getAccessControlTestDAO().findById(simpleObject.getId());
    }


    @Test(expected = NoSuchItemException.class)
    public void unauthorizedAccessAttemptWithEmptyACLAndEmptyAccessControlContext()
            throws NoSuchItemException {
        accessControlContextProvider.setCurrent(accessControlContext1);

        SimpleObject simpleObject = new SimpleObject();

        getAccessControlTestDAO().save(simpleObject);
        getAccessControlTestDAO().revokeAccess(simpleObject.getId(), accessControlContext1.getAccessId());

        accessControlContextProvider.setCurrent(accessControlContext4);

        getAccessControlTestDAO().findById(simpleObject.getId());
    }


    @Test(expected = NoSuchItemException.class)
    public void cantAccessCreatedObjectWithEmptyEmptyAccessControlContext()
            throws NoSuchItemException {
        accessControlContextProvider.setCurrent(accessControlContext4);

        SimpleObject simpleObject = new SimpleObject();

        getAccessControlTestDAO().save(simpleObject);

        getAccessControlTestDAO().findById(simpleObject.getId());
    }


    @Test
    public void grantedAccessAttempt()
            throws NoSuchItemException {
        accessControlContextProvider.setCurrent(accessControlContext1);

        SimpleObject simpleObject = new SimpleObject();

        getAccessControlTestDAO().save(simpleObject);

        getAccessControlTestDAO().grantAccess(simpleObject.getId(), accessControlContext2.getAccessId());

        accessControlContextProvider.setCurrent(accessControlContext2);

        SimpleObject resultObject = getAccessControlTestDAO().findById(simpleObject.getId());

        Assert.assertEquals(resultObject.getId(), simpleObject.getId());
        Assert.assertEquals(2, getAccessControlTestDAO().getAccessIds(resultObject.getId()).size());
    }


    @Test
    public void getList() {
        accessControlContextProvider.setCurrent(accessControlContext1);

        for (int i = 0; i < 10; i++) {
            getAccessControlTestDAO().save(new SimpleObject());
        }

        accessControlContextProvider.setCurrent(accessControlContext2);

        for (int i = 0; i < 5; i++) {
            getAccessControlTestDAO().save(new SimpleObject());
        }

        accessControlContextProvider.setCurrent(accessControlContext1);

        Iterable<SimpleObject> simpleObjectsAvailableToUser1 = getAccessControlTestDAO().findAll();

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

        Iterable<SimpleObject> simpleObjectsAvailableToUser2 = getAccessControlTestDAO().findAll();

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

        getAccessControlTestDAO().save(simpleObject);

        accessControlContextProvider.setCurrent(accessControlContext3);

        SimpleObject resultObject = getAccessControlTestDAO().findById(
                simpleObject.getId());

        Assert.assertEquals(resultObject.getId(), simpleObject.getId());
    }


    @Test
    public void verifyOrderByWorks() {
        accessControlContextProvider.setCurrent(accessControlContext1);

        for (int i = 0; i < 10; i++) {
            getAccessControlTestDAO().save(new SimpleObject());
        }

        List<SimpleObject> orderedItems = getAccessControlTestDAO().findByOrderById();

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
        relatedObject.setRelatedStringValue("foo");

        simpleObject.setRelatedObjectSet(Collections.singleton(relatedObject));

        getAccessControlTestDAO().save(simpleObject);

        simpleObject = new SimpleObject();

        getAccessControlTestDAO().save(simpleObject);

        List<SimpleObject> resultObjects
                = getAccessControlTestDAO().findByHavingRelatedObjectSetWithRelatedStringValue("foo");

        Assert.assertEquals(1, resultObjects.size());
    }


    @Test
    public void checkAnnotationQueryWorks() {
        accessControlContextProvider.setCurrent(accessControlContext1);

        for (int i = 1; i < 10; i++) {
            getAccessControlTestDAO().save(new SimpleObject(i));
        }

        accessControlContextProvider.setCurrent(accessControlContext2);

        for (int i = 2; i < 10; i++) {
            getAccessControlTestDAO().save(new SimpleObject(i));
        }

        accessControlContextProvider.setCurrent(accessControlContext1);

        Assert.assertEquals(3, getAccessControlTestDAO().getByIntValueLessThan(4).size());
        Assert.assertEquals(2, getAccessControlTestDAO().getByIntValueLessThanSpecifyingContext(4, accessControlContext2).size());
        Assert.assertEquals(5, getAccessControlTestDAO().getByIntValueLessThanUsingAdministratorRole(4).size());
        Assert.assertEquals(0, getAccessControlTestDAO().getByIntValueLessThanUsingBogusRole(4).size());
    }
}
