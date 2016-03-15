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

package org.iternine.jeppetto.dao.hibernate.id;


import org.iternine.jeppetto.dao.test.id.NoIdObjectDAO;
import org.iternine.jeppetto.dao.test.id.NoIdObjectDAOTest;
import org.iternine.jeppetto.testsupport.TestContext;

import org.hibernate.SessionFactory;


public class HibernateNoIdObjectDAOTest extends NoIdObjectDAOTest {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private TestContext testContext;


    //-------------------------------------------------------------
    // Methods - Implementation
    //-------------------------------------------------------------

    @Override
    protected NoIdObjectDAO getNoIdObjectDAO() {
        if (testContext == null) {
            testContext = new TestContext("HibernateNoIdObjectDAOTest.spring.xml",
                                          "HibernateDAOTest.test.properties",
                                          "hibernateDAOTest.jdbc.driverClass");
        }

        return (NoIdObjectDAO) testContext.getBean("noIdObjectDAO");
    }


    @Override
    protected void reset() {
        if (testContext != null) {
            testContext.close();

            testContext = null;
        }
    }
}
