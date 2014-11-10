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

package org.iternine.jeppetto.dao.dynamodb.iterable;


import org.iternine.jeppetto.enhance.Enhancer;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


public class QueryIterable<T> extends DynamoDBIterable<T> {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private QueryRequest queryRequest;

    private final Logger logger = LoggerFactory.getLogger(QueryIterable.class);


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public QueryIterable(AmazonDynamoDB dynamoDB, Enhancer<T> enhancer, QueryRequest queryRequest, String hashKeyField) {
        super(dynamoDB, enhancer, hashKeyField);

        this.queryRequest = queryRequest;
    }


    //-------------------------------------------------------------
    // Methods - Implementation
    //-------------------------------------------------------------

    @Override
    public Map<String, AttributeValue> getLastEvaluatedKey() {
        return queryRequest.getExclusiveStartKey(); // We store lastEvaluatedKey in queryRequest's exclusiveStartKey field
    }


    @Override
    protected void setExclusiveStartKey(Map<String, AttributeValue> exclusiveStartKey) {
        queryRequest.setExclusiveStartKey(exclusiveStartKey);
    }


    @Override
    protected Iterator<Map<String, AttributeValue>> fetchItems() {
        QueryResult currentQueryResult = getDynamoDB().query(queryRequest);
        Iterator<Map<String, AttributeValue>> iterator = currentQueryResult.getItems().iterator();

        queryRequest.setExclusiveStartKey(currentQueryResult.getLastEvaluatedKey());    // Prepare for next query

        if (logger.isDebugEnabled()) {
            logger.debug("Queried {} using {}.  Took {} read capacity units, retrieved {} items, more items {} available.",
                         getEnhancer().getBaseClass().getSimpleName(),
                         queryRequest,
                         currentQueryResult.getConsumedCapacity(),
                         currentQueryResult.getCount(),
                         currentQueryResult.getLastEvaluatedKey() == null ? "are not" : "are");
        }

        return iterator;
    }


    @Override
    protected boolean moreAvailable() {
        return queryRequest.getExclusiveStartKey() != null;
    }
}
