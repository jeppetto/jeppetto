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

package org.iternine.jeppetto.dao.hibernate.core;


import org.iternine.jeppetto.dao.GenericDAO;
import org.iternine.jeppetto.dao.test.SimpleObject;
import org.iternine.jeppetto.dao.test.core.OptimisticLockTest;
import org.iternine.jeppetto.testsupport.TestContext;

import org.hibernate.SessionFactory;


public class HibernateOptimisticLockTest extends OptimisticLockTest {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private TestContext testContext;


    //-------------------------------------------------------------
    // Implementation - OptimisticLockTest
    //-------------------------------------------------------------

    @Override
    protected GenericDAO<SimpleObject, String> getGenericDAO() {
        if (testContext == null) {
            testContext = new TestContext("HibernateGenericDAOTest.spring.xml",
                                          "HibernateDAOTest.test.properties",
                                          "hibernateDAOTest.jdbc.driverClass");

        }

        //noinspection unchecked
        return (GenericDAO<SimpleObject, String>) testContext.getBean("hibernateGenericDAO");
    }


    @Override
    protected void reset() {
        if (testContext != null) {
            testContext.close();

            testContext = null;
        }
    }
}
