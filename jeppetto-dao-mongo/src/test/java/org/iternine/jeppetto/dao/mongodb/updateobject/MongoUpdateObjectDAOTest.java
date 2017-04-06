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

package org.iternine.jeppetto.dao.mongodb.updateobject;


import org.iternine.jeppetto.dao.test.updateobject.UpdateObjectDAO;
import org.iternine.jeppetto.dao.test.updateobject.UpdateObjectDAOTest;
import org.iternine.jeppetto.testsupport.MongoDatabaseProvider;
import org.iternine.jeppetto.testsupport.TestContext;

import org.junit.Ignore;
import org.junit.Test;


public class MongoUpdateObjectDAOTest extends UpdateObjectDAOTest {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private TestContext testContext;


    //-------------------------------------------------------------
    // Implementation - UpdateObjectDAOTest
    //-------------------------------------------------------------

    @Override
    protected UpdateObjectDAO getSimpleObjectReferencesDAO() {
        if (testContext == null) {
            testContext = new TestContext("MongoUpdateObjectDAOTest.spring.xml",
                                          "MongoDAOTest.properties",
                                          new MongoDatabaseProvider());
        }

        return (UpdateObjectDAO) testContext.getBean("updateObjectDAO");
    }


    @Override
    protected void reset() {
        if (testContext != null) {
            testContext.close();

            testContext = null;
        }
    }


    //-------------------------------------------------------------
    // Tests
    //-------------------------------------------------------------

    @Ignore("MongoDB doesn't support removing an item by index, only by value.")
    @Test
    public void removeFromExistingListUsingIndex() {
    }


    @Ignore("Need to implement update object extension.")
    @Test
    public void incrementIntValue() {
    }


    @Ignore("Need to implement update object extension.")
    @Test
    public void decrementIntValue() {
    }


    @Ignore("Need to implement update object extension.")
    @Test
    public void incrementLongValue() {
    }


    @Ignore("Need to implement update object extension.")
    @Test
    public void incrementDoubleValue() {
    }


    @Ignore("Not yet implemented...")
    @Test
    public void incrementIntValueAndReturnAfterModified() {
    }


    @Ignore("Not yet implemented...")
    @Test
    public void incrementIntValueAndReturnBeforeModified() {
    }


    @Ignore("Not yet implemented...")
    @Test
    public void incrementIntValueAndReturnNone() {
    }
}
