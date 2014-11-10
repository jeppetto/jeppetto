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

package org.iternine.jeppetto.dao.mongodb.core;


import org.iternine.jeppetto.dao.test.core.ProjectionDAO;
import org.iternine.jeppetto.dao.test.core.ProjectionDAOTest;
import org.iternine.jeppetto.testsupport.MongoDatabaseProvider;
import org.iternine.jeppetto.testsupport.TestContext;

import org.junit.Ignore;
import org.junit.Test;


public class MongoProjectionDAOTest extends ProjectionDAOTest {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private TestContext testContext;


    //-------------------------------------------------------------
    // Implementation - ProjectionDAOTest
    //-------------------------------------------------------------

    @Override
    protected ProjectionDAO getProjectionDAO() {
        if (testContext == null) {
            testContext = new TestContext("MongoProjectionDAOTest.spring.xml",
                                          "MongoDAOTest.properties",
                                          new MongoDatabaseProvider());
        }

        //noinspection unchecked
        return (ProjectionDAO) testContext.getBean("mongoProjectionDAO");
    }


    @Override
    protected void reset() {
        if (testContext != null) {
            testContext.close();

            testContext = null;
        }
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
