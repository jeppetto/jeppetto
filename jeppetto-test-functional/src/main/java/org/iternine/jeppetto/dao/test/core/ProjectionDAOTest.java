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


import org.iternine.jeppetto.dao.test.RelatedObject;
import org.iternine.jeppetto.dao.test.SimpleObject;

import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public abstract class ProjectionDAOTest {

    //-------------------------------------------------------------
    // Methods - Abstract
    //-------------------------------------------------------------

    protected abstract ProjectionDAO getProjectionDAO();

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
    public void countSomeObjects() {
        createData();

        assertEquals(3, getProjectionDAO().countByIntValueLessThan(100));
        assertEquals(2, getProjectionDAO().countByIntValueLessThan(3));
        assertEquals(1, getProjectionDAO().countByIntValueLessThan(2));
        assertEquals(0, getProjectionDAO().countByIntValueLessThan(1));

        assertEquals(0, getProjectionDAO().countByIntValue(0));
        assertEquals(1, getProjectionDAO().countByIntValue(1));
        assertEquals(1, getProjectionDAO().countByIntValue(2));
        assertEquals(1, getProjectionDAO().countByIntValue(3));
    }


    @Test
    public void countSomeObjectsUsingAnnotation() {
        createData();

        assertEquals(3L, getProjectionDAO().doAnAnnotationBasedCount(0));
        assertEquals(2L, getProjectionDAO().doAnAnnotationBasedCount(1));
        assertEquals(1L, getProjectionDAO().doAnAnnotationBasedCount(2));
        assertEquals(0L, getProjectionDAO().doAnAnnotationBasedCount(3));
    }


    @Test
    public void countSomeObjectsUsingAnnotationGreaterThanEquals() {
        createData();

        assertEquals(3L, getProjectionDAO().doAnAnnotationBasedCountGreaterThanEquals(0));
        assertEquals(3L, getProjectionDAO().doAnAnnotationBasedCountGreaterThanEquals(1));
        assertEquals(2L, getProjectionDAO().doAnAnnotationBasedCountGreaterThanEquals(2));
        assertEquals(1L, getProjectionDAO().doAnAnnotationBasedCountGreaterThanEquals(3));
        assertEquals(0L, getProjectionDAO().doAnAnnotationBasedCountGreaterThanEquals(4));
    }


    @Test
    public void countSomeObjectsUsingDslStyleGreaterThanEquals() {
        createData();

        assertEquals(3L, getProjectionDAO().countByIntValueGreaterThanEqual(0));
        assertEquals(3L, getProjectionDAO().countByIntValueGreaterThanEqual(1));
        assertEquals(2L, getProjectionDAO().countByIntValueGreaterThanEqual(2));
        assertEquals(1L, getProjectionDAO().countByIntValueGreaterThanEqual(3));
        assertEquals(0L, getProjectionDAO().countByIntValueGreaterThanEqual(4));
    }


    @Test
    public void countSomeObjectsUsingAnnotationLessThanEquals() {
        createData();

        assertEquals(3L, getProjectionDAO().doAnAnnotationBasedCountLessThanEquals(4));
        assertEquals(3L, getProjectionDAO().doAnAnnotationBasedCountLessThanEquals(3));
        assertEquals(2L, getProjectionDAO().doAnAnnotationBasedCountLessThanEquals(2));
        assertEquals(1L, getProjectionDAO().doAnAnnotationBasedCountLessThanEquals(1));
        assertEquals(0L, getProjectionDAO().doAnAnnotationBasedCountLessThanEquals(0));
    }


    @Test
    public void countSomeObjectsUsingDslStyleLessThanEquals() {
        createData();

        assertEquals(3L, getProjectionDAO().countByIntValueLessThanEqual(4));
        assertEquals(3L, getProjectionDAO().countByIntValueLessThanEqual(3));
        assertEquals(2L, getProjectionDAO().countByIntValueLessThanEqual(2));
        assertEquals(1L, getProjectionDAO().countByIntValueLessThanEqual(1));
        assertEquals(0L, getProjectionDAO().countByIntValueLessThanEqual(0));
    }


    @Test
    public void countAll() {
        createData();

        assertEquals(3L, getProjectionDAO().countAll());
    }


    @Test
    public void sumIntValues() {
        createData();

        assertEquals(6, getProjectionDAO().sumIntValues(), 0.0);
    }


    @Test
    public void averageIntValues() {
        createData();

        assertEquals(2.0, getProjectionDAO().averageIntValues(), 0.0);
    }


    @Test
    public void minIntValue() {
        createData();

        assertEquals(1, getProjectionDAO().minIntValue(), 0.0);
    }


    @Test
    public void maxIntValue() {
        createData();

        assertEquals(3, getProjectionDAO().maxIntValue(), 0.0);
    }


    @Test
    public void countDistinctIntValue() {
        createExtraData();

        assertEquals(5, getProjectionDAO().countAll());
        assertEquals(4, getProjectionDAO().countDistinctAnotherIntValue());
    }


    @Test
    public void countRelatedItems() {
        createData();

        assertEquals(3, getProjectionDAO().countRelatedItems(21));
        assertEquals(2, getProjectionDAO().countRelatedItems(20));
        assertEquals(1, getProjectionDAO().countRelatedItems(15));
        assertEquals(0, getProjectionDAO().countRelatedItems(10));
    }


    //-------------------------------------------------------------
    // Methods - Protected
    //-------------------------------------------------------------

    protected void createData() {
        SimpleObject simpleObject;
        RelatedObject relatedObject;

        simpleObject = new SimpleObject();
        simpleObject.setIntValue(1);
        simpleObject.setAnotherIntValue(1);
        relatedObject = new RelatedObject();
        relatedObject.setRelatedIntValue(20);
        simpleObject.addRelatedObject(relatedObject);
        getProjectionDAO().save(simpleObject);

        simpleObject = new SimpleObject();
        simpleObject.setIntValue(2);
        simpleObject.setAnotherIntValue(2);
        relatedObject = new RelatedObject();
        relatedObject.setRelatedIntValue(15);
        simpleObject.addRelatedObject(relatedObject);
        getProjectionDAO().save(simpleObject);

        simpleObject = new SimpleObject();
        simpleObject.setIntValue(3);
        simpleObject.setAnotherIntValue(3);
        relatedObject = new RelatedObject();
        relatedObject.setRelatedIntValue(10);
        simpleObject.addRelatedObject(relatedObject);
        getProjectionDAO().save(simpleObject);
    }


    protected void createExtraData() {
        createData();

        SimpleObject simpleObject;

        simpleObject = new SimpleObject();
        simpleObject.setIntValue(4);
        simpleObject.setAnotherIntValue(4);
        getProjectionDAO().save(simpleObject);

        simpleObject = new SimpleObject();
        simpleObject.setIntValue(5);
        simpleObject.setAnotherIntValue(4);
        getProjectionDAO().save(simpleObject);
    }
}
