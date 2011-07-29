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

import com.mongodb.MongoException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class DynamicMongoDAOTest {

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
        this.testContext = new TestContext("MongoDAOTest.spring.xml",
                                           "MongoDAOTest.properties",
                                           new MongoDatabaseProvider());

        this.simpleObjectDAO = ((SimpleObjectDAO) testContext.getBean("simpleObjectDAO"));
    }


    @After
    public void tearDown() throws InterruptedException {
        if (testContext != null) {
            testContext.close();
        }
    }


    //-------------------------------------------------------------
    // Methods - Test Cases
    //-------------------------------------------------------------

    @Test
    public void findById()
            throws NoSuchItemException {
        SimpleObject simpleObject = new SimpleObject();

        simpleObjectDAO.save(simpleObject);

        SimpleObject resultObject = simpleObjectDAO.findById(simpleObject.getId());

        assertEquals(resultObject.getId(), simpleObject.getId());
    }


    @Test
    public void findByBogusId() throws NoSuchItemException {
        assertNull(simpleObjectDAO.findById("bogusId"));
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
    @Ignore
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
    @Ignore("currently a bug in mongod is causing an orphaned db")
    public void sumIntValues() {
        createData();
        assertEquals(6.0, 0.0, simpleObjectDAO.sumIntValues());
    }


    @Test
    @Ignore("currently a bug in mongod is causing an orphaned db")
    public void averageIntValues() {
        createData();
        assertEquals(2.0, simpleObjectDAO.averageIntValues(), 0.0);
    }


    @Test
    @Ignore("currently a bug in mongod is causing an orphaned db")
    public void minIntValue() {
        createData();
        assertEquals(1.0, 0.0, simpleObjectDAO.minIntValue());
    }


    @Test
    @Ignore("currently a bug in mongod is causing an orphaned db")
    public void maxIntValue() {
        createData();
        assertEquals(3.0, 0.0, simpleObjectDAO.maxIntValue());
    }


    @Test
    @Ignore("currently a bug in mongod is causing an orphaned db")
    public void countDistinctIntValue() {
        createExtraData();

        // this test has changed since intValue got a unique index
        assertEquals(5, simpleObjectDAO.countIntValue());
        assertEquals(5, simpleObjectDAO.countDistinctIntValue());
    }

    @Test(expected = MongoException.DuplicateKey.class)
    public void createDuplicateOnUniquenessConstraintCausesException() {
        createDuplicateData();
    }

    @Test
    public void findSomeObjects() {
        createData();

        List<SimpleObject> results = simpleObjectDAO.findSomeObjects(Arrays.asList(1, 3, 4));

        assertEquals(2, results.size());
        Assert.assertTrue(results.get(0).getIntValue() == 1 || results.get(0).getIntValue() == 3);
        Assert.assertTrue(results.get(0).getIntValue() != results.get(1).getIntValue());
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
    // Methods - Private
    //-------------------------------------------------------------

    private void createData() {
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

    private void createDuplicateData() {
        createData();
        createData();
    }


    private void createExtraData() {
        createData();

        SimpleObject simpleObject;

        simpleObject = new SimpleObject();
        simpleObject.setIntValue(-1);
        simpleObjectDAO.save(simpleObject);

        simpleObject = new SimpleObject();
        simpleObject.setIntValue(4);
        simpleObjectDAO.save(simpleObject);
    }
}
