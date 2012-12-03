package org.iternine.jeppetto.dao.mongodb;


import org.iternine.jeppetto.test.SimpleObjectDAO;
import org.iternine.jeppetto.test.SummarySimpleObjectDAO;
import org.iternine.jeppetto.test.SummarySimpleObjectDAOTest;
import org.iternine.jeppetto.testsupport.MongoDatabaseProvider;
import org.iternine.jeppetto.testsupport.TestContext;


public class MongoSummarySimpleObjectDAOTest extends SummarySimpleObjectDAOTest {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private TestContext testContext;


    //-------------------------------------------------------------
    // Implementation - SimpleObjectDAOTest
    //-------------------------------------------------------------

    @Override
    protected SimpleObjectDAO getSimpleObjectDAO() {
        if (testContext == null) {
            testContext = new TestContext("MongoDAOTest.spring.xml",
                                          "MongoDAOTest.properties",
                                          new MongoDatabaseProvider());
        }

        return (SimpleObjectDAO) testContext.getBean("simpleObjectDAO");
    }


    @Override
    protected SummarySimpleObjectDAO getSummarySimpleObjectDAO() {
        if (testContext == null) {
            testContext = new TestContext("MongoDAOTest.spring.xml",
                                          "MongoDAOTest.properties",
                                          new MongoDatabaseProvider());
        }

        return (SummarySimpleObjectDAO) testContext.getBean("summarySimpleObjectDAO");
    }


    @Override
    protected void reset() {
        if (testContext != null) {
            testContext.close();

            testContext = null;
        }
    }
}
