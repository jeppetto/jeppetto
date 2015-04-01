/*
 * Copyright (c) 2011-2015 Jeppetto and Jonathan Thompson
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

package org.iternine.jeppetto.dao.dynamodb.extra.expression;


import org.iternine.jeppetto.dao.dynamodb.expression.DynamoDBReservedWords;

import org.junit.Assert;
import org.junit.Test;


public class DynamoDBReservedWordsTest {

    //-------------------------------------------------------------
    // Tests
    //-------------------------------------------------------------

    @Test
    public void checkBuilder() {
        Assert.assertTrue(DynamoDBReservedWords.isReserved("application/json"));
        Assert.assertTrue(DynamoDBReservedWords.isReserved("32"));
        Assert.assertTrue(DynamoDBReservedWords.isReserved("4a"));

        Assert.assertFalse(DynamoDBReservedWords.isReserved("application.json"));
        Assert.assertFalse(DynamoDBReservedWords.isReserved("a4"));
    }
}
