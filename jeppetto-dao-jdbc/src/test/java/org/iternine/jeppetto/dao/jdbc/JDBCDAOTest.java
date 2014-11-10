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

package org.iternine.jeppetto.dao.jdbc;


import org.iternine.jeppetto.dao.NoSuchItemException;
import org.iternine.jeppetto.testsupport.TestContext;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;


public class JDBCDAOTest {

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
        testContext = new TestContext("jdbcDAOTest.spring.xml",
                                      "jdbcDAOTest.test.properties",
                                      "jdbcDAOTest.jdbc.driverClass");

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
    public void findById()
            throws NoSuchItemException {
        SimpleObject simpleObject = new SimpleObject();

        simpleObjectDAO.save(simpleObject);

        Assert.assertNotNull(simpleObject.getId());

        SimpleObject resultObject = simpleObjectDAO.findById(simpleObject.getId());

        Assert.assertEquals(resultObject.getId(), simpleObject.getId());
    }


    @Test(expected = NoSuchItemException.class)
    public void findByBogusId()
            throws NoSuchItemException {
        simpleObjectDAO.findById("bogus");
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
    public void findByTwoValues() {
        SimpleObject newOne = new SimpleObject();
        newOne.setIntValue(1);
        newOne.setAnotherIntValue(20);

        simpleObjectDAO.save(newOne);

        List<SimpleObject> result = simpleObjectDAO.findByIntValueAndAnotherIntValueGreaterThan(1, 10);

        Assert.assertEquals(1, result.size());
        Assert.assertEquals(newOne.getId(), result.get(0).getId());
    }


    @Test(expected = NoSuchItemException.class)
    public void saveAndDelete()
            throws NoSuchItemException {
        SimpleObject simpleObject = new SimpleObject();

        simpleObjectDAO.save(simpleObject);

        SimpleObject resultObject = simpleObjectDAO.findById(simpleObject.getId());

        Assert.assertEquals(resultObject.getId(), simpleObject.getId());

        simpleObjectDAO.delete(simpleObject);

        simpleObjectDAO.findById(simpleObject.getId());
    }
}
