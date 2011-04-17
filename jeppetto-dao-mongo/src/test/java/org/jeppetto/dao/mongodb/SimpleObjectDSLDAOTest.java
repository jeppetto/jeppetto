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

package org.jeppetto.dao.mongodb;


import org.jeppetto.dao.NoSuchItemException;
import org.jeppetto.testsupport.MongoDatabaseProvider;
import org.jeppetto.testsupport.TestContext;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Set;


public class SimpleObjectDSLDAOTest {

    //-------------------------------------------------------------
    // Variables - Private - Static
    //-------------------------------------------------------------

    private TestContext testContext;
    private SimpleObjectDSLDAO simpleObjectDAO;


    //-------------------------------------------------------------
    // Methods - Set-Up / Tear-Down
    //-------------------------------------------------------------

    @Before
    public void setUp() {
        this.testContext = new TestContext("MongoDAOTest.spring.xml",
                                           "MongoDAOTest.properties",
                                           new MongoDatabaseProvider());

        simpleObjectDAO = (SimpleObjectDSLDAO) testContext.getBean("simpleObjectDSLDAO");
    }


    @After
    public void tearDown()
            throws InterruptedException {
        if (testContext != null) {
            testContext.close();
        }
    }


    //-------------------------------------------------------------
    // Methods - Tests
    //-------------------------------------------------------------

    @Test
    public void findById()
            throws NoSuchItemException {
        SimpleObject newOne = new SimpleObject();

        newOne.setIntValue(1);

        simpleObjectDAO.save(newOne);

        SimpleObject result = simpleObjectDAO.findById(newOne.getId());

        Assert.assertEquals(newOne.getIntValue(), result.getIntValue());
    }


    @Test
    public void findByIntValue() {
        SimpleObject newOne = new SimpleObject();

        newOne.setIntValue(1);

        simpleObjectDAO.save(newOne);

        SimpleObject result = simpleObjectDAO.findByIntValue(1);

        Assert.assertEquals(newOne.getId(), result.getId());
    }


    @Test
    public void findByTwoValues() {
        SimpleObject newOne = new SimpleObject();
        newOne.setIntValue(1);
        newOne.setAnotherIntValue(20);

        simpleObjectDAO.save(newOne);

        List<SimpleObject> result = simpleObjectDAO.findByIntValueAndAnotherIntValueGreaterThan(1, 0);

        Assert.assertEquals(1, result.size());
        Assert.assertEquals(newOne.getId(), result.get(0).getId());
    }


    @Test
    public void findByRelatedValue() {
        RelatedObject relatedObject = new RelatedObject();
        relatedObject.setRelatedIntValue(20);

        SimpleObject newOne = new SimpleObject();
        newOne.setIntValue(1);
        newOne.setRelatedObject(relatedObject);

        simpleObjectDAO.save(newOne);

        SimpleObject result = simpleObjectDAO.findByIntValueHavingRelatedObjectWithRelatedIntValue(1, 20);

        Assert.assertEquals(newOne.getId(), result.getId());
    }


    @Test
    public void verifyNullReturnWhenMethodDoesNotDeclareException() {
        SimpleObject result = simpleObjectDAO.findByIntValue(1);

        Assert.assertNull(result);
    }


    @Test(expected = NoSuchItemException.class)
    public void verifyExceptionWhenMethodDeclaresException()
            throws NoSuchItemException {
        simpleObjectDAO.findByAnotherIntValue(1);
    }


    @Test
    public void getSet() {
        SimpleObject newOne = new SimpleObject();

        newOne.setIntValue(1);

        simpleObjectDAO.save(newOne);

        Set<SimpleObject> result = simpleObjectDAO.findByIntValueGreaterThan(0);

        Assert.assertEquals(1, result.size());
    }
}
