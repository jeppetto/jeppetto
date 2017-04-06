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

package org.iternine.jeppetto.dao.dynamodb.examples.gamescore;


import org.iternine.jeppetto.dao.test.examples.gamescore.GameScoreDAO;
import org.iternine.jeppetto.dao.test.examples.gamescore.GameScoreTest;
import org.iternine.jeppetto.dao.test.examples.gamescore.UserProgressDAO;
import org.iternine.jeppetto.testsupport.DynamoDBDatabaseProvider;
import org.iternine.jeppetto.testsupport.TestContext;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


/**
 */
public class DynamoDBGameScoreTest extends GameScoreTest {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private TestContext testContext;
    private List<CreateTableRequest> createTableRequests = new ArrayList<CreateTableRequest>() { {
        add(new CreateTableRequest(Arrays.asList(new AttributeDefinition("userId", ScalarAttributeType.S),
                                                 new AttributeDefinition("gameTitle", ScalarAttributeType.S),
                                                 new AttributeDefinition("topScore", ScalarAttributeType.N)),
                                   "GameScores",
                                   Arrays.asList(new KeySchemaElement("userId", KeyType.HASH),
                                                 new KeySchemaElement("gameTitle", KeyType.RANGE)),
                                   new ProvisionedThroughput(1L, 1L))
                    .withGlobalSecondaryIndexes(new GlobalSecondaryIndex().withIndexName("gameTitleIndex")
                                                                          .withKeySchema(new KeySchemaElement("gameTitle", KeyType.HASH),
                                                                                         new KeySchemaElement("topScore", KeyType.RANGE))
                                                                          .withProjection(new Projection().withProjectionType(ProjectionType.ALL))
                                                                          .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L))));
    } };


    //-------------------------------------------------------------
    // Methods - Implementation
    //-------------------------------------------------------------

    @Override
    protected GameScoreDAO getGameScoreDAO() {
        if (testContext == null) {
            testContext = new TestContext("DynamoDBGameScoreTest.spring.xml",
                                          "DynamoDBTest.properties",
                                          new DynamoDBDatabaseProvider(createTableRequests));
        }

        //noinspection unchecked
        return (GameScoreDAO) testContext.getBean("gameScoreDAO");
    }


    @Override
    protected UserProgressDAO getUserProgressDAO() {
        if (testContext == null) {
            testContext = new TestContext("DynamoDBGameScoreTest.spring.xml",
                                          "DynamoDBTest.properties",
                                          new DynamoDBDatabaseProvider(createTableRequests));
        }

        //noinspection unchecked
        return (UserProgressDAO) testContext.getBean("userProgressDAO");
    }


    @Override
    protected void reset() {
        if (testContext != null) {
            testContext.close();

            testContext = null;
        }
    }
}
