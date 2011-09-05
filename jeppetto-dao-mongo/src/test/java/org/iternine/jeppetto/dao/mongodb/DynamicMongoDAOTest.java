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

package org.iternine.jeppetto.dao.mongodb;


import com.mongodb.MongoException;
import org.iternine.jeppetto.test.DAOTestSuite;
import org.iternine.jeppetto.testsupport.MongoDatabaseProvider;
import org.iternine.jeppetto.testsupport.TestContext;
import org.junit.Ignore;
import org.junit.Test;


public class DynamicMongoDAOTest extends DAOTestSuite {

    //-------------------------------------------------------------
    // Variables - DAOTestSuite Implementation
    //-------------------------------------------------------------

    @Override
    public TestContext getTestContext() {
        return new TestContext("MongoDAOTest.spring.xml", "MongoDAOTest.properties", new MongoDatabaseProvider());
    }


    //-------------------------------------------------------------
    // Methods - Test Cases
    //-------------------------------------------------------------

    @Test(expected = MongoException.DuplicateKey.class)
    public void createDuplicateOnUniquenessConstraintCausesException() {

        super.createData();
        super.createData();
    }

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
