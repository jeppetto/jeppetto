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

package org.iternine.jeppetto.dao.test.examples.forum;


import junit.framework.Assert;
import org.junit.After;
import org.junit.Test;

import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;


/**
 */
public abstract class ForumTest {

    //-------------------------------------------------------------
    // Constants
    //-------------------------------------------------------------

    protected static final String DYNAMODB_FORUM = "Amazon DynamoDB";
    protected static final String DYNAMODB_THREAD_1 = "DynamoDB Thread 1";
    protected static final String DYNAMODB_THREAD_2 = "DynamoDB Thread 2";
    protected static final String S3_FORUM = "Amazon S3";
    protected static final String S3_THREAD_1 = "S3 Thread 1";
    protected static final String USER_A = "User A";
    protected static final String USER_B = "User B";
    protected static final Date oneDayAgo = new Date((new Date()).getTime() - (1*24*60*60*1000));
    protected static final Date sevenDaysAgo = new Date((new Date()).getTime() - (7*24*60*60*1000));
    protected static final Date fourteenDaysAgo = new Date((new Date()).getTime() - (14*24*60*60*1000));
    protected static final Date fifteenDaysAgo = new Date((new Date()).getTime() - (15*24*60*60*1000));
    protected static final Date twentyOneDaysAgo = new Date((new Date()).getTime() - (21*24*60*60*1000));


    //-------------------------------------------------------------
    // Methods - Abstract
    //-------------------------------------------------------------

    protected abstract ForumDAO getForumDAO();

    protected abstract ThreadDAO getThreadDAO();

    protected abstract ReplyDAO getReplyDAO();

    protected abstract void reset();


    //-------------------------------------------------------------
    // Methods - Test Lifecycle
    //-------------------------------------------------------------

    @After
    public void after() {
        reset();
    }


    //-------------------------------------------------------------
    // Tests
    //-------------------------------------------------------------

    @Test
    public void findRepliesInLast15Days() {
        createData();

        List<Reply> result = getReplyDAO().findByIdAndReplyDateGreaterThan(DYNAMODB_FORUM + "#" + DYNAMODB_THREAD_1,
                                                                           fifteenDaysAgo);

        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(USER_B, result.get(0).getPostedBy());
    }


    @Test
    public void findRepliesPostedWithinTimePeriod() {
        createData();

        List<Reply> result = getReplyDAO().findByIdAndReplyDateBetween(DYNAMODB_FORUM + "#" + DYNAMODB_THREAD_1,
                                                                       fifteenDaysAgo, sevenDaysAgo);

        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(USER_B, result.get(0).getPostedBy());
    }


    @Test
    public void findRepliesPostedByUserA() {
        createData();

        List<Reply> result = getReplyDAO().findByIdAndPostedBy(DYNAMODB_FORUM + "#" + DYNAMODB_THREAD_2, USER_A);

        Assert.assertNotNull(result);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(USER_A, result.get(0).getPostedBy());
    }


    //-------------------------------------------------------------
    // Methods - Protected
    //-------------------------------------------------------------

    protected void createData() {
        // Add Forums
        getForumDAO().save(new Forum(DYNAMODB_FORUM, "Amazon Web Services", 2, 4, 1000));
        getForumDAO().save(new Forum(S3_FORUM, "Amazon Web Services", 1, 0, 0));

        // Add Threads
        getThreadDAO().save(new Thread(DYNAMODB_FORUM, DYNAMODB_THREAD_1, "DynamoDB thread 1 message", USER_A, 10, 2, 0,
                                       new HashSet<String>(Arrays.asList("index", "primarykey", "table")), fourteenDaysAgo));
        getThreadDAO().save(new Thread(DYNAMODB_FORUM, DYNAMODB_THREAD_2, "DynamoDB thread 2 message", USER_A, 20, 2, 0,
                                       new HashSet<String>(Arrays.asList("index", "primarykey", "rangekey")), twentyOneDaysAgo));
        getThreadDAO().save(new Thread(S3_FORUM, S3_THREAD_1, "S3 thread 1 message", USER_A, 0, 0, 0,
                                       new HashSet<String>(Arrays.asList("largeobjects", "multipart upload")), sevenDaysAgo));
        
        // Add Replies
        getReplyDAO().save(new Reply(DYNAMODB_FORUM + "#" + DYNAMODB_THREAD_1, "DynamoDB Thread 1 Reply 1 text", USER_A, twentyOneDaysAgo));
        getReplyDAO().save(new Reply(DYNAMODB_FORUM + "#" + DYNAMODB_THREAD_1, "DynamoDB Thread 1 Reply 2 text", USER_B, fourteenDaysAgo));
        getReplyDAO().save(new Reply(DYNAMODB_FORUM + "#" + DYNAMODB_THREAD_2, "DynamoDB Thread 2 Reply 1 text", USER_A, sevenDaysAgo));
        getReplyDAO().save(new Reply(DYNAMODB_FORUM + "#" + DYNAMODB_THREAD_2, "DynamoDB Thread 2 Reply 2 text", USER_A, oneDayAgo));
    }
}
