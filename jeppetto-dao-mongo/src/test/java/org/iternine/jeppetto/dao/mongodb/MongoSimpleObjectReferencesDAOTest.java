package org.iternine.jeppetto.dao.mongodb;


import org.iternine.jeppetto.test.SimpleObjectReferencesDAO;
import org.iternine.jeppetto.test.SimpleObjectReferencesDAOTest;
import org.iternine.jeppetto.test.SimpleObjectDAO;
import org.iternine.jeppetto.testsupport.MongoDatabaseProvider;
import org.iternine.jeppetto.testsupport.TestContext;


public class MongoSimpleObjectReferencesDAOTest extends SimpleObjectReferencesDAOTest {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private TestContext testContext;


    //-------------------------------------------------------------
    // Implementation - SimpleObjectDAOTest
    //-------------------------------------------------------------

    @Override
    protected SimpleObjectReferencesDAO getSimpleObjectReferencesDAO() {
        if (testContext == null) {
            testContext = new TestContext("SimpleObjectReferencesDAOTest.spring.xml",
                                          "MongoDAOTest.properties",
                                          new MongoDatabaseProvider());
        }

        return (SimpleObjectReferencesDAO) testContext.getBean("simpleObjectReferencesDAO");
    }


    @Override
    protected void reset() {
        if (testContext != null) {
            testContext.close();

            testContext = null;
        }
    }
}
