package org.iternine.jeppetto.dao.mongodb.referenceset;


import org.iternine.jeppetto.dao.test.referenceset.ReferenceSetDAO;
import org.iternine.jeppetto.dao.test.referenceset.ReferenceSetDAOTest;
import org.iternine.jeppetto.testsupport.MongoDatabaseProvider;
import org.iternine.jeppetto.testsupport.TestContext;


public class MongoReferenceSetDAOTest extends ReferenceSetDAOTest {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private TestContext testContext;


    //-------------------------------------------------------------
    // Implementation - ReferenceSetDAOTest
    //-------------------------------------------------------------

    @Override
    protected ReferenceSetDAO getSimpleObjectReferencesDAO() {
        if (testContext == null) {
            testContext = new TestContext("MongoReferenceSetDAOTest.spring.xml",
                                          "MongoDAOTest.properties",
                                          new MongoDatabaseProvider());
        }

        return (ReferenceSetDAO) testContext.getBean("referenceSetDAO");
    }


    @Override
    protected void reset() {
        if (testContext != null) {
            testContext.close();

            testContext = null;
        }
    }
}
