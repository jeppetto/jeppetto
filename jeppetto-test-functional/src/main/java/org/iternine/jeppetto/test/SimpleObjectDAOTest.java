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

package org.iternine.jeppetto.test;


import org.iternine.jeppetto.dao.NoSuchItemException;

import org.junit.After;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public abstract class SimpleObjectDAOTest {

    //-------------------------------------------------------------
    // Methods - Abstract
    //-------------------------------------------------------------

    protected abstract SimpleObjectDAO getSimpleObjectDAO();

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
    public void findById() throws NoSuchItemException {
        SimpleObject simpleObject = new SimpleObject();

        getSimpleObjectDAO().save(simpleObject);

        SimpleObject resultObject = getSimpleObjectDAO().findById(simpleObject.getId());

        assertEquals(resultObject.getId(), simpleObject.getId());
    }


    @Test(expected = NoSuchItemException.class)
    public void findByBogusId()
            throws NoSuchItemException {
        getSimpleObjectDAO().findById("bogusId");
    }


    @Test
    public void findByIntValue()
            throws NoSuchItemException {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(20);
        getSimpleObjectDAO().save(simpleObject);

        SimpleObject resultObject = getSimpleObjectDAO().findByIntValue(20);

        assertEquals(resultObject.getId(), simpleObject.getId());
    }


    @Test
    public void findByLongValue()
            throws NoSuchItemException {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setLongValue(1409040442249560600L);
        getSimpleObjectDAO().save(simpleObject);

        SimpleObject resultObject = getSimpleObjectDAO().findByLongValue(1409040442249560600L);

        assertEquals(resultObject.getLongValue(), simpleObject.getLongValue());
    }


    @Test
    public void findSimpleObject()
            throws NoSuchItemException {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(21);
        simpleObject.setSimpleEnum(SimpleEnum.EnumValue);
        getSimpleObjectDAO().save(simpleObject);

        SimpleObject resultObject = getSimpleObjectDAO().findSimpleObject(21);

        assertEquals(resultObject.getId(), simpleObject.getId());
    }


    @Test
    public void countSomeObjects() {
        createData();

        assertEquals(3, getSimpleObjectDAO().countByIntValueLessThan(100));
        assertEquals(2, getSimpleObjectDAO().countByIntValueLessThan(3));
        assertEquals(1, getSimpleObjectDAO().countByIntValueLessThan(2));
        assertEquals(0, getSimpleObjectDAO().countByIntValueLessThan(1));

        assertEquals(0, getSimpleObjectDAO().countByIntValue(0));
        assertEquals(1, getSimpleObjectDAO().countByIntValue(1));
        assertEquals(1, getSimpleObjectDAO().countByIntValue(2));
        assertEquals(1, getSimpleObjectDAO().countByIntValue(3));
    }


    @Test
    public void countSomeObjectsUsingAnnotation() {
        createData();

        assertEquals(3L, getSimpleObjectDAO().doAnAnnotationBasedCount(0));
        assertEquals(2L, getSimpleObjectDAO().doAnAnnotationBasedCount(1));
        assertEquals(1L, getSimpleObjectDAO().doAnAnnotationBasedCount(2));
        assertEquals(0L, getSimpleObjectDAO().doAnAnnotationBasedCount(3));
    }


    @Test
    public void countSomeObjectsUsingAnnotationGreaterThanEquals() {
        createData();

        assertEquals(3L, getSimpleObjectDAO().doAnAnnotationBasedCountGreaterThanEquals(0));
        assertEquals(3L, getSimpleObjectDAO().doAnAnnotationBasedCountGreaterThanEquals(1));
        assertEquals(2L, getSimpleObjectDAO().doAnAnnotationBasedCountGreaterThanEquals(2));
        assertEquals(1L, getSimpleObjectDAO().doAnAnnotationBasedCountGreaterThanEquals(3));
        assertEquals(0L, getSimpleObjectDAO().doAnAnnotationBasedCountGreaterThanEquals(4));
    }


    @Test
    public void countSomeObjectsUsingDslStyleGreaterThanEquals() {
        createData();

        assertEquals(3L, getSimpleObjectDAO().countByIntValueGreaterThanEqual(0));
        assertEquals(3L, getSimpleObjectDAO().countByIntValueGreaterThanEqual(1));
        assertEquals(2L, getSimpleObjectDAO().countByIntValueGreaterThanEqual(2));
        assertEquals(1L, getSimpleObjectDAO().countByIntValueGreaterThanEqual(3));
        assertEquals(0L, getSimpleObjectDAO().countByIntValueGreaterThanEqual(4));
    }


    @Test
    public void countSomeObjectsUsingAnnotationLessThanEquals() {
        createData();

        assertEquals(3L, getSimpleObjectDAO().doAnAnnotationBasedCountLessThanEquals(4));
        assertEquals(3L, getSimpleObjectDAO().doAnAnnotationBasedCountLessThanEquals(3));
        assertEquals(2L, getSimpleObjectDAO().doAnAnnotationBasedCountLessThanEquals(2));
        assertEquals(1L, getSimpleObjectDAO().doAnAnnotationBasedCountLessThanEquals(1));
        assertEquals(0L, getSimpleObjectDAO().doAnAnnotationBasedCountLessThanEquals(0));
    }


    @Test
    public void countSomeObjectsUsingDslStyleLessThanEquals() {
        createData();

        assertEquals(3L, getSimpleObjectDAO().countByIntValueLessThanEqual(4));
        assertEquals(3L, getSimpleObjectDAO().countByIntValueLessThanEqual(3));
        assertEquals(2L, getSimpleObjectDAO().countByIntValueLessThanEqual(2));
        assertEquals(1L, getSimpleObjectDAO().countByIntValueLessThanEqual(1));
        assertEquals(0L, getSimpleObjectDAO().countByIntValueLessThanEqual(0));
    }


    @Test
    public void countAll() {
        createData();

        assertEquals(3L, getSimpleObjectDAO().countAll());
    }


    @Test
    public void sumIntValues() {
        createData();

        assertEquals(6, getSimpleObjectDAO().sumIntValues(), 0.0);
    }


    @Test
    public void averageIntValues() {
        createData();

        assertEquals(2.0, getSimpleObjectDAO().averageIntValues(), 0.0);
    }


    @Test
    public void minIntValue() {
        createData();

        assertEquals(1, getSimpleObjectDAO().minIntValue(), 0.0);
    }


    @Test
    public void maxIntValue() {
        createData();

        assertEquals(3, getSimpleObjectDAO().maxIntValue(), 0.0);
    }


    @Test
    public void countDistinctIntValue() {
        createExtraData();

        assertEquals(5, getSimpleObjectDAO().countAll());
        assertEquals(4, getSimpleObjectDAO().countDistinctIntValue());
    }


    @Test
    public void findSomeObjects() {
        createData();

        List<SimpleObject> results = getSimpleObjectDAO().findSomeObjects(Arrays.asList(1, 3, 4));

        assertEquals(2, results.size());
        assertTrue(results.get(0).getIntValue() == 1 || results.get(0).getIntValue() == 3);
        assertTrue(results.get(0).getIntValue() != results.get(1).getIntValue());
    }


    @Test
    public void findAndSort() {
        createData();

        List<SimpleObject> results = getSimpleObjectDAO().findAndSortRelatedItems(19);

        assertEquals(2, results.size());
        assertEquals(3, results.get(0).getIntValue());
        assertEquals(2, results.get(1).getIntValue());
    }

    @Test
    public void findAndSortReturnIterable() {
        createData();

        Iterable<SimpleObject> results = getSimpleObjectDAO().findAndSortRelatedItemsIterable(19);
        Iterator<SimpleObject> i = results.iterator();

        assertTrue(i.hasNext());
        assertEquals(3, i.next().getIntValue());
        assertTrue(i.hasNext());
        assertEquals(2, i.next().getIntValue());
        assertFalse(i.hasNext());
    }


    @Test
    public void countRelatedItems() {
        createData();

        assertEquals(3, getSimpleObjectDAO().countRelatedItems(21));
        assertEquals(2, getSimpleObjectDAO().countRelatedItems(20));
        assertEquals(1, getSimpleObjectDAO().countRelatedItems(15));
        assertEquals(0, getSimpleObjectDAO().countRelatedItems(10));
    }


    @Test
    public void limit() {
        createData();

        List<SimpleObject> results = getSimpleObjectDAO().limitRelatedItems(19, 1);

        assertEquals(1, results.size());
        assertEquals(2, results.get(0).getIntValue());
    }


    @Test
    public void limitAndSkip() {
        createData();

        List<SimpleObject> results = getSimpleObjectDAO().limitAndSkipRelatedItems(19, 1, 1);

        assertEquals(1, results.size());
        assertEquals(3, results.get(0).getIntValue());
    }


    @Test
    public void limitAndSkipDSL() {
        createData();

        List<SimpleObject> results = getSimpleObjectDAO().findByHavingRelatedObjectsWithRelatedIntValueLessThanOrderByIntValueAndLimitAndSkip(19, 1, 1);

        assertEquals(1, results.size());
        assertEquals(3, results.get(0).getIntValue());
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
        getSimpleObjectDAO().save(simpleObject);

        simpleObject = new SimpleObject();
        simpleObject.setIntValue(2);
        relatedObject = new RelatedObject();
        relatedObject.setRelatedIntValue(15);
        simpleObject.addRelatedObject(relatedObject);
        getSimpleObjectDAO().save(simpleObject);

        simpleObject = new SimpleObject();
        simpleObject.setIntValue(3);
        relatedObject = new RelatedObject();
        relatedObject.setRelatedIntValue(10);
        simpleObject.addRelatedObject(relatedObject);
        getSimpleObjectDAO().save(simpleObject);
    }


    protected void createExtraData() {
        createData();

        SimpleObject simpleObject;

        simpleObject = new SimpleObject();
        simpleObject.setIntValue(-1);
        getSimpleObjectDAO().save(simpleObject);

        simpleObject = new SimpleObject();
        simpleObject.setIntValue(3);
        getSimpleObjectDAO().save(simpleObject);
    }
}
