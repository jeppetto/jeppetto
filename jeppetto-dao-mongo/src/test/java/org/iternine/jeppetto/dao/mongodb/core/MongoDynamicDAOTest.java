/*
 * Copyright (c) 2011-2017 Jeppetto and Jonathan Thompson
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

package org.iternine.jeppetto.dao.mongodb.core;


import org.iternine.jeppetto.dao.test.core.DynamicDAO;
import org.iternine.jeppetto.dao.test.core.DynamicDAOTest;
import org.iternine.jeppetto.testsupport.MongoDatabaseProvider;
import org.iternine.jeppetto.testsupport.TestContext;


public class MongoDynamicDAOTest extends DynamicDAOTest {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private TestContext testContext;


    //-------------------------------------------------------------
    // Implementation - DynamicDAOTest
    //-------------------------------------------------------------

    @Override
    protected DynamicDAO getDynamicDAO() {
        if (testContext == null) {
            testContext = new TestContext("MongoDAOTest.spring.xml",
                                          "MongoDAOTest.properties",
                                          new MongoDatabaseProvider());
        }

        return (DynamicDAO) testContext.getBean("mongoDynamicDAO");
    }


    @Override
    protected void reset() {
        if (testContext != null) {
            testContext.close();

            testContext = null;
        }
    }
}
