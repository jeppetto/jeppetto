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

package org.iternine.jeppetto;


import org.iternine.jeppetto.test.AccessControlTest;
import org.iternine.jeppetto.test.AccessControllableObjectDAO;
import org.iternine.jeppetto.testsupport.TestContext;


public class HibernateAccessControlTest extends AccessControlTest {

    //-------------------------------------------------------------
    // Variables - Private - Static
    //-------------------------------------------------------------

    private TestContext testContext;


    //-------------------------------------------------------------
    // Implementation - SimpleObjectDAOTest
    //-------------------------------------------------------------

    @Override
    protected AccessControllableObjectDAO getAccessControllableObjectDAO() {
        if (testContext == null) {
            testContext = new TestContext("AccessControlTest.spring.xml",
                                          "HibernateDAOTest.test.properties",
                                          "hibernateDAOTest.jdbc.driverClass");

        }

        return (AccessControllableObjectDAO) testContext.getBean("accessControllableObjectDAO");
    }


    @Override
    protected void reset() {
        if (testContext != null) {
            testContext.close();

            testContext = null;
        }
    }
}
