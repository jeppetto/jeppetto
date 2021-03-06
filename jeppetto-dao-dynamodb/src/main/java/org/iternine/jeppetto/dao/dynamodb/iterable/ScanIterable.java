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
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;


public class ScanIterable<T> extends DynamoDBIterable<T> {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private ScanRequest scanRequest;
    private Collection<String> keyFields;

    private final Logger logger = LoggerFactory.getLogger(ScanIterable.class);


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public ScanIterable(AmazonDynamoDB dynamoDB, Enhancer<T> enhancer, ScanRequest scanRequest, Collection<String> keyFields) {
        super(dynamoDB, enhancer);

        this.scanRequest = scanRequest;
        this.keyFields = keyFields;
    }


    //-------------------------------------------------------------
    // Methods - Implementation
    //-------------------------------------------------------------

    @Override
    protected void setExclusiveStartKey(Map<String, AttributeValue> exclusiveStartKey) {
        scanRequest.setExclusiveStartKey(exclusiveStartKey);
    }


    @Override
    protected Iterator<Map<String, AttributeValue>> fetchItems() {
        ScanResult currentScanResult = getDynamoDB().scan(scanRequest);
        Iterator<Map<String, AttributeValue>> iterator = currentScanResult.getItems().iterator();

        scanRequest.setExclusiveStartKey(currentScanResult.getLastEvaluatedKey());    // Prepare for next query

        if (logger.isDebugEnabled()) {
            logger.debug("Scanned {} using {}.  Took {} read capacity units, retrieved {} items, more items {} available.",
                         getEnhancer().getBaseClass().getSimpleName(),
                         scanRequest,
                         currentScanResult.getConsumedCapacity(),
                         currentScanResult.getCount(),
                         currentScanResult.getLastEvaluatedKey() == null ? "are not" : "are");
        }

        return iterator;
    }


    @Override
    protected boolean moreAvailable() {
        return scanRequest.getExclusiveStartKey() != null;
    }


    @Override
    protected Collection<String> getKeyFields() {
        return keyFields;
    }


    @Override
    protected String getHashKeyField() {
        throw new JeppettoException("HashKeyField not used by ScanIterable");
    }


    //-------------------------------------------------------------
    // Methods - Override
    //-------------------------------------------------------------

    @Override
    public String getPosition(boolean removeHashKey) {
        if (removeHashKey) {
            throw new JeppettoException("ScanIterable doesn't support hash key removal.");
        }

        return super.getPosition(false);
    }
}
