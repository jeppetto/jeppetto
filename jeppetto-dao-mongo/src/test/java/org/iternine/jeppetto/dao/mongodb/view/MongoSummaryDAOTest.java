package org.iternine.jeppetto.dao.mongodb.view;


import org.iternine.jeppetto.dao.test.core.DynamicDAO;
import org.iternine.jeppetto.dao.test.view.SummaryDAO;
import org.iternine.jeppetto.dao.test.view.SummaryDAOTest;
import org.iternine.jeppetto.testsupport.MongoDatabaseProvider;
import org.iternine.jeppetto.testsupport.TestContext;


public class MongoSummaryDAOTest extends SummaryDAOTest {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private TestContext testContext;


    //-------------------------------------------------------------
    // Implementation - SummaryDAOTest
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
    protected SummaryDAO getSummaryDAO() {
        if (testContext == null) {
            testContext = new TestContext("MongoDAOTest.spring.xml",
                                          "MongoDAOTest.properties",
                                          new MongoDatabaseProvider());
        }

        return (SummaryDAO) testContext.getBean("mongoSummaryDAO");
    }


    @Override
    protected void reset() {
        if (testContext != null) {
            testContext.close();

            testContext = null;
        }
    }
}
