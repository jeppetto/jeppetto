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

package org.iternine.jeppetto.dao.test.core;


import org.iternine.jeppetto.dao.NoSuchItemException;
import org.iternine.jeppetto.dao.test.RelatedObject;
import org.iternine.jeppetto.dao.test.SimpleObject;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public abstract class DynamicDAOTest {

    //-------------------------------------------------------------
    // Methods - Abstract
    //-------------------------------------------------------------

    protected abstract DynamicDAO getDynamicDAO();

    protected abstract void reset();


    //-------------------------------------------------------------
    // Methods - Test Lifecycle
    //-------------------------------------------------------------

    @After
    public void after() {
        reset();
    }


    //-------------------------------------------------------------
    // Methods - Test Cases
    //-------------------------------------------------------------

    @Test
    public void findByIntValue() {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(20);

        getDynamicDAO().save(simpleObject);

        SimpleObject resultObject = getDynamicDAO().findByIntValue(20);

        assertEquals(resultObject.getId(), simpleObject.getId());
    }


    @Test
    public void findByLongValue()
            throws NoSuchItemException {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setLongValue(1409040442249560600L);

        getDynamicDAO().save(simpleObject);

        SimpleObject resultObject = getDynamicDAO().findByLongValue(1409040442249560600L);

        assertEquals(resultObject.getLongValue(), simpleObject.getLongValue());
    }


    @Test
    public void findByTwoValues() {
        SimpleObject newOne = new SimpleObject();
        newOne.setIntValue(1);
        newOne.setAnotherIntValue(20);

        getDynamicDAO().save(newOne);

        List<SimpleObject> result = getDynamicDAO().findByIntValueAndAnotherIntValueGreaterThan(1, 0);

        Assert.assertEquals(1, result.size());
        Assert.assertEquals(newOne.getId(), result.get(0).getId());
    }


    @Test
    public void findByRelatedValue() {
        RelatedObject relatedObject1 = new RelatedObject();
        relatedObject1.setRelatedIntValue(20);

        SimpleObject simpleObject1 = new SimpleObject();
        simpleObject1.setIntValue(1);
        simpleObject1.setRelatedObject(relatedObject1);

        getDynamicDAO().save(simpleObject1);

        RelatedObject relatedObject2 = new RelatedObject();
        relatedObject2.setRelatedIntValue(30);

        SimpleObject simpleObject2 = new SimpleObject();
        simpleObject1.setIntValue(2);
        simpleObject2.setRelatedObject(relatedObject2);

        getDynamicDAO().save(simpleObject2);

        SimpleObject result = getDynamicDAO().findByHavingRelatedObjectWithRelatedIntValue(20);

        Assert.assertEquals(1, result.getIntValue());
        Assert.assertEquals(20, result.getRelatedObject().getRelatedIntValue());
    }


    @Test
    public void verifyNullReturnWhenMethodDoesNotDeclareException() {
        SimpleObject result = getDynamicDAO().findByIntValue(1);

        Assert.assertNull(result);
    }


    @Test(expected = NoSuchItemException.class)
    public void verifyExceptionWhenMethodDeclaresException()
            throws NoSuchItemException {
        getDynamicDAO().findByAnotherIntValue(1);
    }


    @Test
    public void verifySort() {
        SimpleObject simpleObject;

        simpleObject = new SimpleObject();
        simpleObject.setIntValue(1);
        getDynamicDAO().save(simpleObject);

        simpleObject = new SimpleObject();
        simpleObject.setIntValue(2);
        getDynamicDAO().save(simpleObject);

        simpleObject = new SimpleObject();
        simpleObject.setIntValue(3);
        getDynamicDAO().save(simpleObject);

        simpleObject = new SimpleObject();
        simpleObject.setIntValue(4);
        getDynamicDAO().save(simpleObject);

        List<SimpleObject> result = getDynamicDAO().findByOrderByIntValueDesc();

        Assert.assertEquals(4, result.size());
        Assert.assertEquals(4, result.get(0).getIntValue());
        Assert.assertEquals(3, result.get(1).getIntValue());
        Assert.assertEquals(2, result.get(2).getIntValue());
        Assert.assertEquals(1, result.get(3).getIntValue());
    }


    @Test
    public void getSet() {
        SimpleObject newOne = new SimpleObject();

        newOne.setIntValue(1);

        getDynamicDAO().save(newOne);

        Set<SimpleObject> result = getDynamicDAO().findByIntValueGreaterThan(0);

        Assert.assertEquals(1, result.size());
    }
    
    
    @Test
    public void findSomeObjects() {
        createData();

        List<SimpleObject> results = getDynamicDAO().findByIntValueWithin(Arrays.asList(1, 3, 4));

        assertEquals(2, results.size());
        assertTrue(results.get(0).getIntValue() == 1 || results.get(0).getIntValue() == 3);
        assertTrue(results.get(0).getIntValue() != results.get(1).getIntValue());
    }


    @Test
    public void findAndSort() {
        createData();

        List<SimpleObject> results = getDynamicDAO().findByHavingRelatedObjectsWithRelatedIntValueLessThanOrderByIntValueDesc(19);

        assertEquals(2, results.size());
        assertEquals(3, results.get(0).getIntValue());
        assertEquals(2, results.get(1).getIntValue());
    }

    @Test
    public void findAndSortReturnIterable() {
        createData();

        Iterable<SimpleObject> results = getDynamicDAO().findByHavingRelatedObjectsWithRelatedIntValueLessThanOrderByIntValue(19);
        Iterator<SimpleObject> i = results.iterator();

        assertTrue(i.hasNext());
        assertEquals(2, i.next().getIntValue());
        assertTrue(i.hasNext());
        assertEquals(3, i.next().getIntValue());
        assertFalse(i.hasNext());
    }


    @Test
    public void limit() {
        createData();

        List<SimpleObject> results = getDynamicDAO().findByHavingRelatedObjectsWithRelatedIntValueLessThanOrderByIntValueAndLimit(19, 1);

        assertEquals(1, results.size());
        assertEquals(2, results.get(0).getIntValue());
    }


    @Test
    public void limitAndSkip() {
        createData();

        List<SimpleObject> results = getDynamicDAO().findByHavingRelatedObjectsWithRelatedIntValueLessThanOrderByIntValueAndLimitAndSkip(19, 1, 1);

        assertEquals(1, results.size());
        assertEquals(3, results.get(0).getIntValue());
    }


    @Test
    // TODO: Move to functional test (requires hibernate implementation)
    public void saveMultipleThenDeleteSome()
            throws NoSuchItemException {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(1234);

        getDynamicDAO().save(simpleObject);

        SimpleObject simpleObject2 = new SimpleObject();
        simpleObject2.setIntValue(2345);

        getDynamicDAO().save(simpleObject2);

        SimpleObject simpleObject3 = new SimpleObject();
        simpleObject2.setIntValue(3456);

        getDynamicDAO().save(simpleObject3);

        Iterable<SimpleObject> results = getDynamicDAO().findAll();

        int resultCount = 0;
        // noinspection UnusedDeclaration
        for (SimpleObject ignore : results) {
            resultCount++;
        }

        Assert.assertTrue(resultCount == 3);

        getDynamicDAO().deleteByIntValueWithin(Arrays.asList(1234, 2345));

        Iterable<SimpleObject> results2 = getDynamicDAO().findAll();

        int resultCount2 = 0;
        // noinspection UnusedDeclaration
        for (SimpleObject ignore : results2) {
            resultCount2++;
        }

        Assert.assertTrue(resultCount2 == 1);
    }


    @Test
    public void findByRelatedObjectIsNotNull() {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(1234);

        getDynamicDAO().save(simpleObject);

        List<SimpleObject> results = getDynamicDAO().findByRelatedObjectIsNotNull();

        Assert.assertEquals(0, results.size());
    }


    //-------------------------------------------------------------
    // Methods - Protected
    //-------------------------------------------------------------

    protected void createData() {
        SimpleObject simpleObject;
        RelatedObject relatedObject;

        simpleObject = new SimpleObject();
        simpleObject.setIntValue(1);
        relatedObject = new RelatedObject();
        relatedObject.setRelatedIntValue(20);
        simpleObject.addRelatedObject(relatedObject);
        getDynamicDAO().save(simpleObject);

        simpleObject = new SimpleObject();
        simpleObject.setIntValue(2);
        relatedObject = new RelatedObject();
        relatedObject.setRelatedIntValue(15);
        simpleObject.addRelatedObject(relatedObject);
        getDynamicDAO().save(simpleObject);

        simpleObject = new SimpleObject();
        simpleObject.setIntValue(3);
        relatedObject = new RelatedObject();
        relatedObject.setRelatedIntValue(10);
        simpleObject.addRelatedObject(relatedObject);
        getDynamicDAO().save(simpleObject);
    }
}
