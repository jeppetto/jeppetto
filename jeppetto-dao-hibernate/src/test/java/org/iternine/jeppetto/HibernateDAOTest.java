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

package org.iternine.jeppetto;


import org.iternine.jeppetto.test.DAOTestSuite;
import org.iternine.jeppetto.testsupport.TestContext;

import org.iternine.jeppetto.dao.NoSuchItemException;
import org.junit.Test;


public class HibernateDAOTest extends DAOTestSuite {

    //-------------------------------------------------------------
    // Methods - DAOTestSuite Implementation
    //-------------------------------------------------------------

    @Override
    public TestContext getTestContext() {
        return new TestContext("HibernateDAOTest.spring.xml",
                               "HibernateDAOTest.test.properties",
                               "hibernateDAOTest.jdbc.driverClass");
    }

    //-------------------------------------------------------------
    // Methods - Test Cases
    //-------------------------------------------------------------

    @Test(expected = NoSuchItemException.class)
    @Override
    public void findByBogusId()
            throws NoSuchItemException {
        simpleObjectDAO.findById("bogusId");
    }
}
