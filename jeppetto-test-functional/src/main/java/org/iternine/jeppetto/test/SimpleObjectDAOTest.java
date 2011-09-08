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
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public abstract class SimpleObjectDAOTest {

    //-------------------------------------------------------------
    // Variables - Protected
    //-------------------------------------------------------------

    protected SimpleObjectDAO simpleObjectDAO;



    //-------------------------------------------------------------
    // Methods - Abstract
    //-------------------------------------------------------------

    protected abstract SimpleObjectDAO getSimpleObjectDAO();

    protected abstract void reset();


    //-------------------------------------------------------------
    // Methods - Set-Up / Tear-Down
    //-------------------------------------------------------------

    @Before
    public void before() {
        simpleObjectDAO = getSimpleObjectDAO();
    }


    @After
    public void after() {
        reset();

        simpleObjectDAO = null;
    }


    //-------------------------------------------------------------
    // Methods - Test Cases
    //-------------------------------------------------------------

    @Test
    public void findById() throws NoSuchItemException {
        SimpleObject simpleObject = new SimpleObject();

        simpleObjectDAO.save(simpleObject);

        SimpleObject resultObject = simpleObjectDAO.findById(simpleObject.getId());

        assertEquals(resultObject.getId(), simpleObject.getId());
    }


    @Test(expected = NoSuchItemException.class)
    public void findByBogusId()
            throws NoSuchItemException {
        simpleObjectDAO.findById("bogusId");
    }


    @Test
    public void findByIntValue()
            throws NoSuchItemException {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(20);
        simpleObjectDAO.save(simpleObject);

        SimpleObject resultObject = simpleObjectDAO.findByIntValue(20);

        assertEquals(resultObject.getId(), simpleObject.getId());
    }


    @Test
    public void findSimpleObject()
            throws NoSuchItemException {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(21);
        simpleObject.setSimpleEnum(SimpleEnum.EnumValue);
        simpleObjectDAO.save(simpleObject);

        SimpleObject resultObject = simpleObjectDAO.findSimpleObject(21);

        assertEquals(resultObject.getId(), simpleObject.getId());
    }


    @Test
    public void countSomeObjects() {
        createData();

        assertEquals(3, simpleObjectDAO.countByIntValueLessThan(100));
        assertEquals(2, simpleObjectDAO.countByIntValueLessThan(3));
        assertEquals(1, simpleObjectDAO.countByIntValueLessThan(2));
        assertEquals(0, simpleObjectDAO.countByIntValueLessThan(1));

        assertEquals(0, simpleObjectDAO.countByIntValue(0));
        assertEquals(1, simpleObjectDAO.countByIntValue(1));
        assertEquals(1, simpleObjectDAO.countByIntValue(2));
        assertEquals(1, simpleObjectDAO.countByIntValue(3));
    }


    @Test
    public void countSomeObjectsUsingAnnotation() {
        createData();

        assertEquals(3L, simpleObjectDAO.doAnAnnotationBasedCount(0));
        assertEquals(2L, simpleObjectDAO.doAnAnnotationBasedCount(1));
        assertEquals(1L, simpleObjectDAO.doAnAnnotationBasedCount(2));
        assertEquals(0L, simpleObjectDAO.doAnAnnotationBasedCount(3));
    }


    @Test
    public void countSomeObjectsUsingAnnotationGreaterThanEquals() {
        createData();

        assertEquals(3L, simpleObjectDAO.doAnAnnotationBasedCountGreaterThanEquals(0));
        assertEquals(3L, simpleObjectDAO.doAnAnnotationBasedCountGreaterThanEquals(1));
        assertEquals(2L, simpleObjectDAO.doAnAnnotationBasedCountGreaterThanEquals(2));
        assertEquals(1L, simpleObjectDAO.doAnAnnotationBasedCountGreaterThanEquals(3));
        assertEquals(0L, simpleObjectDAO.doAnAnnotationBasedCountGreaterThanEquals(4));
    }


    @Test
    public void countSomeObjectsUsingDslStyleGreaterThanEquals() {
        createData();

        assertEquals(3L, simpleObjectDAO.countByIntValueGreaterThanEqual(0));
        assertEquals(3L, simpleObjectDAO.countByIntValueGreaterThanEqual(1));
        assertEquals(2L, simpleObjectDAO.countByIntValueGreaterThanEqual(2));
        assertEquals(1L, simpleObjectDAO.countByIntValueGreaterThanEqual(3));
        assertEquals(0L, simpleObjectDAO.countByIntValueGreaterThanEqual(4));
    }


    @Test
    public void countSomeObjectsUsingAnnotationLessThanEquals() {
        createData();

        assertEquals(3L, simpleObjectDAO.doAnAnnotationBasedCountLessThanEquals(4));
        assertEquals(3L, simpleObjectDAO.doAnAnnotationBasedCountLessThanEquals(3));
        assertEquals(2L, simpleObjectDAO.doAnAnnotationBasedCountLessThanEquals(2));
        assertEquals(1L, simpleObjectDAO.doAnAnnotationBasedCountLessThanEquals(1));
        assertEquals(0L, simpleObjectDAO.doAnAnnotationBasedCountLessThanEquals(0));
    }


    @Test
    public void countSomeObjectsUsingDslStyleLessThanEquals() {
        createData();

        assertEquals(3L, simpleObjectDAO.countByIntValueLessThanEqual(4));
        assertEquals(3L, simpleObjectDAO.countByIntValueLessThanEqual(3));
        assertEquals(2L, simpleObjectDAO.countByIntValueLessThanEqual(2));
        assertEquals(1L, simpleObjectDAO.countByIntValueLessThanEqual(1));
        assertEquals(0L, simpleObjectDAO.countByIntValueLessThanEqual(0));
    }


    @Test
    public void countAll() {
        createData();

        assertEquals(3L, simpleObjectDAO.countAll());
    }


    @Test
    public void sumIntValues() {
        createData();

        assertEquals(6, simpleObjectDAO.sumIntValues(), 0.0);
    }


    @Test
    public void averageIntValues() {
        createData();

        assertEquals(2.0, simpleObjectDAO.averageIntValues(), 0.0);
    }


    @Test
    public void minIntValue() {
        createData();

        assertEquals(1, simpleObjectDAO.minIntValue(), 0.0);
    }


    @Test
    public void maxIntValue() {
        createData();

        assertEquals(3, simpleObjectDAO.maxIntValue(), 0.0);
    }


    @Test
    public void countDistinctIntValue() {
        createExtraData();

        assertEquals(5, simpleObjectDAO.countAll());
        assertEquals(4, simpleObjectDAO.countDistinctIntValue());
    }


    @Test
    public void findSomeObjects() {
        createData();

        List<SimpleObject> results = simpleObjectDAO.findSomeObjects(Arrays.asList(1, 3, 4));

        assertEquals(2, results.size());
        assertTrue(results.get(0).getIntValue() == 1 || results.get(0).getIntValue() == 3);
        assertTrue(results.get(0).getIntValue() != results.get(1).getIntValue());
    }


    @Test
    public void findAndSort() {
        createData();

        List<SimpleObject> results = simpleObjectDAO.findAndSortRelatedItems(19);

        assertEquals(2, results.size());
        assertEquals(3, results.get(0).getIntValue());
        assertEquals(2, results.get(1).getIntValue());
    }

    @Test
    public void findAndSortReturnIterable() {
        createData();

        Iterable<SimpleObject> results = simpleObjectDAO.findAndSortRelatedItemsIterable(19);
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

        assertEquals(3, simpleObjectDAO.countRelatedItems(21));
        assertEquals(2, simpleObjectDAO.countRelatedItems(20));
        assertEquals(1, simpleObjectDAO.countRelatedItems(15));
        assertEquals(0, simpleObjectDAO.countRelatedItems(10));
    }


    @Test
    public void limit() {
        createData();

        List<SimpleObject> results = simpleObjectDAO.limitRelatedItems(19, 1);

        assertEquals(1, results.size());
        assertEquals(2, results.get(0).getIntValue());
    }


    @Test
    public void limitAndSkip() {
        createData();

        List<SimpleObject> results = simpleObjectDAO.limitAndSkipRelatedItems(19, 1, 1);

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
        simpleObjectDAO.save(simpleObject);

        simpleObject = new SimpleObject();
        simpleObject.setIntValue(2);
        relatedObject = new RelatedObject();
        relatedObject.setRelatedIntValue(15);
        simpleObject.addRelatedObject(relatedObject);
        simpleObjectDAO.save(simpleObject);

        simpleObject = new SimpleObject();
        simpleObject.setIntValue(3);
        relatedObject = new RelatedObject();
        relatedObject.setRelatedIntValue(10);
        simpleObject.addRelatedObject(relatedObject);
        simpleObjectDAO.save(simpleObject);
    }


    protected void createExtraData() {
        createData();

        SimpleObject simpleObject;

        simpleObject = new SimpleObject();
        simpleObject.setIntValue(-1);
        simpleObjectDAO.save(simpleObject);

        simpleObject = new SimpleObject();
        simpleObject.setIntValue(3);
        simpleObjectDAO.save(simpleObject);
    }
}
