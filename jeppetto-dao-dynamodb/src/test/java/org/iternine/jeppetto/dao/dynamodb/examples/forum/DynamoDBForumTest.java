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

package org.iternine.jeppetto.dao.dynamodb.examples.forum;


import org.iternine.jeppetto.dao.dynamodb.iterable.DynamoDBIterable;
import org.iternine.jeppetto.dao.test.examples.forum.ForumDAO;
import org.iternine.jeppetto.dao.test.examples.forum.ForumTest;
import org.iternine.jeppetto.dao.test.examples.forum.Reply;
import org.iternine.jeppetto.dao.test.examples.forum.ReplyDAO;
import org.iternine.jeppetto.dao.test.examples.forum.ThreadDAO;
import org.iternine.jeppetto.testsupport.DynamoDBDatabaseProvider;
import org.iternine.jeppetto.testsupport.TestContext;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;


/**
 */
public class DynamoDBForumTest extends ForumTest {


    //-------------------------------------------------------------
    // Constants
    //-------------------------------------------------------------

    public static final String FORUM_TEST_SPRING_XML = "DynamoDBForumTest.spring.xml";


    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private TestContext testContext;
    private List<CreateTableRequest> createTableRequests = new ArrayList<CreateTableRequest>() { {
        add(new CreateTableRequest(Collections.singletonList(new AttributeDefinition("name", ScalarAttributeType.S)),
                                   "Forum",
                                   Collections.singletonList(new KeySchemaElement("name", KeyType.HASH)),
                                   new ProvisionedThroughput(1L, 1L)));
        add(new CreateTableRequest(Arrays.asList(new AttributeDefinition("forumName", ScalarAttributeType.S),
                                                 new AttributeDefinition("subject", ScalarAttributeType.S)),
                                   "Thread",
                                   Arrays.asList(new KeySchemaElement("forumName", KeyType.HASH), new KeySchemaElement("subject", KeyType.RANGE)),
                                   new ProvisionedThroughput(1L, 1L)));
        add(new CreateTableRequest(Arrays.asList(new AttributeDefinition("id", ScalarAttributeType.S),
                                                 new AttributeDefinition("replyDate", ScalarAttributeType.N),
                                                 new AttributeDefinition("postedBy", ScalarAttributeType.S)),
                                   "Reply",
                                   Arrays.asList(new KeySchemaElement("id", KeyType.HASH), new KeySchemaElement("replyDate", KeyType.RANGE)),
                                   new ProvisionedThroughput(1L, 1L))
                    .withLocalSecondaryIndexes(new LocalSecondaryIndex().withIndexName("postedBy-Index")
                                                       .withKeySchema(new KeySchemaElement("id", KeyType.HASH),
                                                                      new KeySchemaElement("postedBy", KeyType.RANGE))
                                                       .withProjection(new Projection().withProjectionType(ProjectionType.ALL))));
    } };


    //-------------------------------------------------------------
    // Methods - Implementation
    //-------------------------------------------------------------

    @Override
    protected ForumDAO getForumDAO() {
        if (testContext == null) {
            testContext = new TestContext(FORUM_TEST_SPRING_XML, "DynamoDBTest.properties", new DynamoDBDatabaseProvider(createTableRequests));
        }

        //noinspection unchecked
        return (ForumDAO) testContext.getBean("forumDAO");
    }


    @Override
    protected ThreadDAO getThreadDAO() {
        if (testContext == null) {
            testContext = new TestContext(FORUM_TEST_SPRING_XML, "DynamoDBTest.properties", new DynamoDBDatabaseProvider(createTableRequests));
        }

        //noinspection unchecked
        return (ThreadDAO) testContext.getBean("threadDAO");
    }


    @Override
    protected ReplyDAO getReplyDAO() {
        if (testContext == null) {
            testContext = new TestContext(FORUM_TEST_SPRING_XML, "DynamoDBTest.properties", new DynamoDBDatabaseProvider(createTableRequests));
        }

        //noinspection unchecked
        return (ReplyDAO) testContext.getBean("replyDAO");
    }


    @Override
    protected void reset() {
        if (testContext != null) {
            testContext.close();

            testContext = null;
        }
    }


    //-------------------------------------------------------------
    // Tests
    //-------------------------------------------------------------

    @Test
    public void testPosition() {
        createData();
        createAdditionalData();

        int pageSize = 2;
        int page = 0;
        int itemCounter;
        String queryPosition = null;
        int totalItems = 0;

        do {
            Iterable<Reply> iterable = getReplyDAO().findByIdAndLimit(DYNAMODB_FORUM + "#" + DYNAMODB_THREAD_1, pageSize + 1);
            ((DynamoDBIterable) iterable).setPosition(queryPosition);

            page++;
            itemCounter = 0;

            for (Iterator<Reply> iterator = iterable.iterator(); iterator.hasNext(); ) {
                Reply reply = iterator.next();

                Assert.assertEquals(DYNAMODB_FORUM + "#" + DYNAMODB_THREAD_1, reply.getId());

                itemCounter++;
                totalItems++;

                if (itemCounter == pageSize) {
                    queryPosition = ((DynamoDBIterable) iterable).getPosition();

                    if (iterator.hasNext()) {
                        // another page
                        break;
                    }
                }
            }
        } while (queryPosition != null);

        Assert.assertEquals(3, page);
        Assert.assertEquals(6, totalItems);
    }


    @Test
    public void testPaging() {
        createData();
        createAdditionalData();

        String replyId = DYNAMODB_FORUM + "#" + DYNAMODB_THREAD_1;
        int pageSize = 2;

        int totalItems = 0;

        DynamoDBIterable<Reply> iterable = (DynamoDBIterable<Reply>) getReplyDAO().findByIdAndLimit(replyId, pageSize);

        iterable.setLimit(pageSize);

        for (Reply reply : iterable) {
            Assert.assertEquals(replyId, reply.getId());

            totalItems++;
        }

        Assert.assertEquals(2, totalItems);
        Assert.assertTrue(iterable.hasResultsPastLimit());
    }


    @Test
    public void testPosition2() {
        createData();
        createAdditionalData();

        String replyId = DYNAMODB_FORUM + "#" + DYNAMODB_THREAD_1;
        int pageSize = 2;

        int page = 0;
        String queryPosition = null;
        int totalItems = 0;

        DynamoDBIterable<Reply> iterable;

        do {
            iterable = (DynamoDBIterable<Reply>) getReplyDAO().findByIdAndLimit(replyId, pageSize + 1);

            iterable.setPosition(queryPosition);
            iterable.setLimit(pageSize);

            page++;

            for (Reply reply : iterable) {
                Assert.assertEquals(replyId, reply.getId());

                totalItems++;
            }

            queryPosition = iterable.getPosition();
        } while (iterable.hasResultsPastLimit());

        Assert.assertEquals(3, page);
        Assert.assertEquals(6, totalItems);
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private void createAdditionalData() {
        Date seventeenDaysAgo = new Date((new Date()).getTime() - (17*24*60*60*1000));
        Date tenDaysAgo = new Date((new Date()).getTime() - (10*24*60*60*1000));

        // Add more Replies
        getReplyDAO().save(new Reply(DYNAMODB_FORUM + "#" + DYNAMODB_THREAD_1, "DynamoDB Thread 1 Reply 3 text", USER_A, seventeenDaysAgo));
        getReplyDAO().save(new Reply(DYNAMODB_FORUM + "#" + DYNAMODB_THREAD_1, "DynamoDB Thread 1 Reply 4 text", USER_B, tenDaysAgo));
        getReplyDAO().save(new Reply(DYNAMODB_FORUM + "#" + DYNAMODB_THREAD_1, "DynamoDB Thread 2 Reply 5 text", USER_A, sevenDaysAgo));
        getReplyDAO().save(new Reply(DYNAMODB_FORUM + "#" + DYNAMODB_THREAD_1, "DynamoDB Thread 2 Reply 6 text", USER_A, oneDayAgo));
    }
}
