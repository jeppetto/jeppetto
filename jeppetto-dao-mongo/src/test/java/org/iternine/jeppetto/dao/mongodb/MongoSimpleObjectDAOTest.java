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

package org.iternine.jeppetto.dao.mongodb;


import org.iternine.jeppetto.dao.JeppettoException;
import org.iternine.jeppetto.dao.NoSuchItemException;
import org.iternine.jeppetto.dao.OptimisticLockException;
import org.iternine.jeppetto.test.SimpleObject;
import org.iternine.jeppetto.test.SimpleObjectDAO;
import org.iternine.jeppetto.test.SimpleObjectDAOTest;
import org.iternine.jeppetto.testsupport.MongoDatabaseProvider;
import org.iternine.jeppetto.testsupport.TestContext;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;


public class MongoSimpleObjectDAOTest extends SimpleObjectDAOTest {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private TestContext testContext;


    //-------------------------------------------------------------
    // Implementation - SimpleObjectDAOTest
    //-------------------------------------------------------------

    @Override
    protected SimpleObjectDAO getSimpleObjectDAO() {
        if (testContext == null) {
            testContext = new TestContext("MongoDAOTest.spring.xml",
                                          "MongoDAOTest.properties",
                                          new MongoDatabaseProvider());
        }

        return (SimpleObjectDAO) testContext.getBean("simpleObjectDAO");
    }


    @Override
    protected void reset() {
        if (testContext != null) {
            testContext.close();

            testContext = null;
        }
    }


    //-------------------------------------------------------------
    // Methods - Test Cases
    //-------------------------------------------------------------

    @Test(expected = JeppettoException.class)
    public void createDuplicateOnUniquenessConstraintCausesException() {

        super.createData();
        super.createData();
    }


    @Test(expected = JeppettoException.class)
    public void idReuseShouldFail() {
        SimpleObject simpleObject1 = new SimpleObject();
        simpleObject1.setId("a");
        simpleObject1.setIntValue(-1);
        getSimpleObjectDAO().save(simpleObject1);

        SimpleObject simpleObject2 = new SimpleObject();
        simpleObject2.setId("a");
        simpleObject2.setIntValue(3);
        getSimpleObjectDAO().save(simpleObject2);
    }


    @Test(expected = OptimisticLockException.class)
    public void optimisticLockException() {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setId("a");
        getSimpleObjectDAO().save(simpleObject);

        SimpleObject result1 = getSimpleObjectDAO().findById("a");
        SimpleObject result2 = getSimpleObjectDAO().findById("a");

        result1.setIntValue(1);
        result2.setIntValue(2);

        getSimpleObjectDAO().save(result1);
        getSimpleObjectDAO().save(result2);
    }



    @Test
    // TODO: Move to functional test (requires hibernate implementation)
    public void saveMultipleThenDeleteSome()
            throws NoSuchItemException {
        SimpleObject simpleObject = new SimpleObject();
        simpleObject.setIntValue(1234);

        getSimpleObjectDAO().save(simpleObject);

        SimpleObject simpleObject2 = new SimpleObject();
        simpleObject2.setIntValue(2345);

        getSimpleObjectDAO().save(simpleObject2);

        SimpleObject simpleObject3 = new SimpleObject();
        simpleObject2.setIntValue(3456);

        getSimpleObjectDAO().save(simpleObject3);

        Iterable<SimpleObject> results = getSimpleObjectDAO().findAll();

        int resultCount = 0;
        // noinspection UnusedDeclaration
        for (SimpleObject ignore : results) {
            resultCount++;
        }

        Assert.assertTrue(resultCount == 3);

        getSimpleObjectDAO().deleteByIntValueWithin(Arrays.asList(1234, 2345));

        Iterable<SimpleObject> results2 = getSimpleObjectDAO().findAll();

        int resultCount2 = 0;
        // noinspection UnusedDeclaration
        for (SimpleObject ignore : results2) {
            resultCount2++;
        }

        Assert.assertTrue(resultCount2 == 1);
    }


    //-------------------------------------------------------------
    // Methods - Overrides
    //-------------------------------------------------------------

    @Test
    @Override
    @Ignore("currently a bug in mongod is causing an orphaned db")
    public void sumIntValues() {
    }


    @Test
    @Override
    @Ignore("currently a bug in mongod is causing an orphaned db")
    public void averageIntValues() {
    }


    @Test
    @Override
    @Ignore("currently a bug in mongod is causing an orphaned db")
    public void minIntValue() {
    }


    @Test
    @Override
    @Ignore("currently a bug in mongod is causing an orphaned db")
    public void maxIntValue() {
    }

    @Test
    @Override
    @Ignore("currently a bug in mongod is causing an orphaned db")
    public void countDistinctIntValue() {
    }
}
