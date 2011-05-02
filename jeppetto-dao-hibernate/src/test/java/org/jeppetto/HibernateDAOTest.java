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

package org.jeppetto;


import org.jeppetto.testsupport.TestContext;

import org.jeppetto.dao.NoSuchItemException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;


public class HibernateDAOTest {

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
        testContext = new TestContext("HibernateDAOTest.spring.xml",
                                      "HibernateDAOTest.test.properties",
                                      "hibernateDAOTest.jdbc.driverClass");

        simpleObjectDAO = (SimpleObjectDAO) testContext.getBean("transactionalSimpleObjectDAO");
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
    public void findById()
            throws NoSuchItemException {
        SimpleObject simpleObject = new SimpleObject();

        simpleObjectDAO.save(simpleObject);

        SimpleObject resultObject = simpleObjectDAO.findById(simpleObject.getId());

        Assert.assertEquals(resultObject.getId(), simpleObject.getId());
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

        Assert.assertEquals(resultObject.getId(), simpleObject.getId());
    }


    @Test
    public void countSomeObjects() {
        createData();
        Assert.assertEquals(3, simpleObjectDAO.countByIntValueLessThan(100));
        Assert.assertEquals(2, simpleObjectDAO.countByIntValueLessThan(3));
        Assert.assertEquals(1, simpleObjectDAO.countByIntValueLessThan(2));
        Assert.assertEquals(0, simpleObjectDAO.countByIntValueLessThan(1));

        Assert.assertEquals(0, simpleObjectDAO.countByIntValue(0));
        Assert.assertEquals(1, simpleObjectDAO.countByIntValue(1));
        Assert.assertEquals(1, simpleObjectDAO.countByIntValue(2));
        Assert.assertEquals(1, simpleObjectDAO.countByIntValue(3));
    }


    @Test
    public void countSomeObjectsUsingAnnotation() {
        createData();
        Assert.assertEquals(3, simpleObjectDAO.doAnAnnotationBasedCount(0));
        Assert.assertEquals(2, simpleObjectDAO.doAnAnnotationBasedCount(1));
        Assert.assertEquals(1, simpleObjectDAO.doAnAnnotationBasedCount(2));
        Assert.assertEquals(0, simpleObjectDAO.doAnAnnotationBasedCount(3));
    }


    @Test
    public void countAll() {
        createData();
        Assert.assertEquals(3, simpleObjectDAO.countAll());
    }


    @Test
    public void sumIntValues() {
        createData();
        Assert.assertEquals(6, simpleObjectDAO.sumIntValues());
    }


    @Test
    public void averageIntValues() {
        createData();
        Assert.assertEquals(2.0, simpleObjectDAO.averageIntValues(), 0.0);
    }


    @Test
    public void minIntValue() {
        createData();
        Assert.assertEquals(1, simpleObjectDAO.minIntValue());
    }


    @Test
    public void maxIntValue() {
        createData();
        Assert.assertEquals(3, simpleObjectDAO.maxIntValue());
    }


    @Test
    public void countDistinctIntValue() {
        createExtraData();

        Assert.assertEquals(5, simpleObjectDAO.countAll());
        Assert.assertEquals(4, simpleObjectDAO.countDistinctIntValue());
    }


    @Test
    public void findSomeObjects() {
        createData();

        List<SimpleObject> results = simpleObjectDAO.findSomeObjects(Arrays.asList(1, 3, 4));

        Assert.assertEquals(2, results.size());
        Assert.assertTrue(results.get(0).getIntValue() == 1 || results.get(0).getIntValue() == 3);
        Assert.assertTrue(results.get(0).getIntValue() != results.get(1).getIntValue());
    }


    @Test
    public void findAndSort() {
        createData();

        List<SimpleObject> results = simpleObjectDAO.findAndSortRelatedItems(19);

        Assert.assertEquals(2, results.size());
        Assert.assertEquals(2, results.get(0).getIntValue());
        Assert.assertEquals(3, results.get(1).getIntValue());
    }


    @Test
    public void countRelatedItems() {
        createData();
        Assert.assertEquals(3, simpleObjectDAO.countRelatedItems(21));
        Assert.assertEquals(2, simpleObjectDAO.countRelatedItems(20));
        Assert.assertEquals(1, simpleObjectDAO.countRelatedItems(15));
        Assert.assertEquals(0, simpleObjectDAO.countRelatedItems(10));
    }


    @Test
    public void limit() {
        createData();

        List<SimpleObject> results = simpleObjectDAO.limitRelatedItems(19, 1);

        Assert.assertEquals(1, results.size());
        Assert.assertEquals(2, results.get(0).getIntValue());
    }


    @Test
    public void limitAndSkip() {
        createData();

        List<SimpleObject> results = simpleObjectDAO.limitAndSkipRelatedItems(19, 1, 1);

        Assert.assertEquals(1, results.size());
        Assert.assertEquals(3, results.get(0).getIntValue());
    }


    //-------------------------------------------------------------
    // Methods - Test Cases
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


    private void createExtraData() {
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
