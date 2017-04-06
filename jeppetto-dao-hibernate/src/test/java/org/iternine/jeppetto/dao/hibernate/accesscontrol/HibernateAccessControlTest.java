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

package org.iternine.jeppetto.dao.hibernate.accesscontrol;


import org.iternine.jeppetto.dao.test.accesscontrol.AccessControlTest;
import org.iternine.jeppetto.dao.test.accesscontrol.DefaultAccessObjectDAO;
import org.iternine.jeppetto.dao.test.accesscontrol.IdentifiedCreatableObjectDAO;
import org.iternine.jeppetto.dao.test.accesscontrol.RoleCreatableObjectDAO;
import org.iternine.jeppetto.testsupport.TestContext;


public class HibernateAccessControlTest extends AccessControlTest {

    //-------------------------------------------------------------
    // Variables - Private - Static
    //-------------------------------------------------------------

    private TestContext testContext;


    //-------------------------------------------------------------
    // Implementation - AccessControlTest
    //-------------------------------------------------------------

    @Override
    protected DefaultAccessObjectDAO getDefaultAccessObjectDAO() {
        ensureTestContextExists();

        return (DefaultAccessObjectDAO) testContext.getBean("defaultAccessObjectDAO");
    }


    @Override
    protected IdentifiedCreatableObjectDAO getIdentifiedCreatableObjectDAO() {
        ensureTestContextExists();

        return (IdentifiedCreatableObjectDAO) testContext.getBean("identifiedCreatableObjectDAO");
    }


    @Override
    protected RoleCreatableObjectDAO getRoleCreatableObjectDAO() {
        ensureTestContextExists();

        return (RoleCreatableObjectDAO) testContext.getBean("roleCreatableObjectDAO");
    }


    @Override
    protected void reset() {
        if (testContext != null) {
            testContext.close();

            testContext = null;
        }
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private void ensureTestContextExists() {
        if (testContext == null) {
            testContext = new TestContext("HibernateAccessControlTest.spring.xml",
                                          "HibernateDAOTest.test.properties",
                                          "hibernateDAOTest.jdbc.driverClass");
        }
    }
}
