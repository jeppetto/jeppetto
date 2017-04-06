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

package org.iternine.jeppetto.dao.dynamodb.iterable;


import org.iternine.jeppetto.dao.JeppettoException;
import org.iternine.jeppetto.enhance.Enhancer;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.ConsumedCapacity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class BatchGetIterable<T> extends DynamoDBIterable<T> {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private BatchGetItemRequest batchGetItemRequest;
    private String tableName;

    private final Logger logger = LoggerFactory.getLogger(QueryIterable.class);


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public BatchGetIterable(AmazonDynamoDB dynamoDB, Enhancer<T> enhancer, BatchGetItemRequest batchGetItemRequest, String tableName) {
        super(dynamoDB, enhancer);

        this.batchGetItemRequest = batchGetItemRequest;
        this.tableName = tableName;
    }


    //-------------------------------------------------------------
    // Methods - Implementation
    //-------------------------------------------------------------

    @Override
    protected void setExclusiveStartKey(Map<String, AttributeValue> exclusiveStartKey) {
        throw new JeppettoException("ExclusiveStartKey not used by BatchGet");
    }


    @Override
    protected Iterator<Map<String, AttributeValue>> fetchItems() {
        // TODO: logging and catch dynamodb exception...

        BatchGetItemResult currentBatchGetItemResult = getDynamoDB().batchGetItem(batchGetItemRequest);
        Iterator<Map<String, AttributeValue>> iterator = currentBatchGetItemResult.getResponses().get(tableName).iterator();

        batchGetItemRequest.withRequestItems(currentBatchGetItemResult.getUnprocessedKeys());    // Prepare for next query

        if (logger.isDebugEnabled()) {
            List<ConsumedCapacity> consumedCapacities = currentBatchGetItemResult.getConsumedCapacity();

            logger.debug("Queried {} using {}.  Took {} read capacity units, retrieved {} items, more items {} available.",
                         getEnhancer().getBaseClass().getSimpleName(),
                         batchGetItemRequest,
                         consumedCapacities == null ? null : consumedCapacities.get(0), // Only expecting 1 table
                         currentBatchGetItemResult.getResponses().get(tableName).size(),
                         currentBatchGetItemResult.getUnprocessedKeys() == null ? "are not" : "are");
        }

        return iterator;
    }


    @Override
    protected boolean moreAvailable() {
        return batchGetItemRequest.getRequestItems() != null && batchGetItemRequest.getRequestItems().size() > 0;
    }


    @Override
    protected Collection<String> getKeyFields() {
        throw new JeppettoException("KeyFields not used by BatchGet");
    }


    @Override
    protected String getHashKeyField() {
        throw new JeppettoException("HashKeyField not used by BatchGet");
    }


    //-------------------------------------------------------------
    // Methods - Override
    //-------------------------------------------------------------

    @Override
    public String getPosition(boolean removeHashKey) {
        if (removeHashKey) {
            throw new JeppettoException("BatchGetIterable doesn't support hash key removal.");
        }

        return super.getPosition(false);
    }
}
