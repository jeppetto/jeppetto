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


import org.iternine.jeppetto.dao.JeppettoException;
import org.iternine.jeppetto.dao.dynamodb.DynamoDBPersistable;
import org.iternine.jeppetto.enhance.Enhancer;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.util.Base64;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;


public abstract class DynamoDBIterable<T> implements Iterable<T> {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private AmazonDynamoDB dynamoDB;
    private Enhancer<T> enhancer;
    private String hashKeyField;
    private int limit = -1;
    private DynamoDBIterator dynamoDBIterator;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public DynamoDBIterable(AmazonDynamoDB dynamoDB, Enhancer<T> enhancer, String hashKeyField) {
        this.dynamoDB = dynamoDB;
        this.enhancer = enhancer;
        this.hashKeyField = hashKeyField;
    }


    //-------------------------------------------------------------
    // Methods - Abstract
    //-------------------------------------------------------------

    protected abstract Map<String, AttributeValue> getLastEvaluatedKey();

    protected abstract void setExclusiveStartKey(Map<String, AttributeValue> exclusiveStartKey);

    protected abstract Iterator<Map<String, AttributeValue>> fetchItems();

    protected abstract boolean moreAvailable();


    //-------------------------------------------------------------
    // Implementation - Iterable
    //-------------------------------------------------------------

    @Override
    public Iterator<T> iterator() {
        dynamoDBIterator = new DynamoDBIterator(limit);

        return dynamoDBIterator;
    }


    //-------------------------------------------------------------
    // Methods - Public
    //-------------------------------------------------------------

    public String getPosition() {
        return getPosition(false);
    }


    public String getPosition(boolean removeHashKey) {
        Map<String, AttributeValue> lastExaminedKey = getLastExaminedKey();

        if (lastExaminedKey == null) {
            return null;
        }

        StringBuilder sb = new StringBuilder();

        try {
            for (Map.Entry<String, AttributeValue> entry : lastExaminedKey.entrySet()) {
                if (removeHashKey && entry.getKey().equals(hashKeyField)) {
                    continue;
                }

                if (sb.length() > 0) {
                    sb.append("&");
                }

                sb.append(entry.getKey()).append('=').append(encode(entry.getValue()));
            }

            return URLEncoder.encode(Base64.encodeAsString(sb.toString().getBytes()), StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);      // Unexpected since UTF-8 is the system standard.
        }
    }


    public void setPosition(String position) {
        setPosition(position, null);
    }


    public void setPosition(String position, String hashKeyValue) {
        if (dynamoDBIterator != null) {
            throw new JeppettoException("setPosition() only valid on a new DynamoDBIterable.");
        }

        if (position == null) {
            return;
        }

        try {
            byte[] decodedBytes = Base64.decode(URLDecoder.decode(position, StandardCharsets.UTF_8.name()));
            String[] attributePairs = new String(decodedBytes).split("&");

            if (attributePairs.length == 0) {
                return;
            }

            Map<String, AttributeValue> exclusiveStartKey = new HashMap<String, AttributeValue>();

            for (String attributePair : attributePairs) {
                String[] parts = attributePair.split("=");

                if (parts.length != 2) {
                    throw new JeppettoException("Corrupted position: " + position + "; found attribute: " + attributePair);
                }

                exclusiveStartKey.put(parts[0], decode(parts[1]));
            }

            if (hashKeyValue != null) {
                // TODO: support types other than just 'S'?
                exclusiveStartKey.put(hashKeyField, new AttributeValue(hashKeyValue));
            }

            setExclusiveStartKey(exclusiveStartKey);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);      // Unexpected since UTF-8 is the system standard.
        }
    }


    public void setLimit(int limit) {
        if (dynamoDBIterator != null) {
            throw new JeppettoException("setLimit() only valid on a new DynamoDBIterable.");
        }

        if (limit < 1) {
            throw new JeppettoException("limit value must be a positive integer");
        }

        this.limit = limit;

        // TODO: Should this limit be applied to the underlying query/scan?  If so, add 'plus one' parameter
    }


    public boolean hasResultsPastLimit() {
        if (limit == -1) {
            throw new JeppettoException("An iterable limit wasn't specified with setLimit()");
        }

        // TODO: the result is only valid if the iterator was finished.  Should we try to detect and error if not?
        return dynamoDBIterator.hasNext0();
    }


    //-------------------------------------------------------------
    // Methods - Protected
    //-------------------------------------------------------------

    protected AmazonDynamoDB getDynamoDB() {
        return dynamoDB;
    }


    protected Enhancer<T> getEnhancer() {
        return enhancer;
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private Map<String, AttributeValue> getLastExaminedKey() {
        // TODO: update based on value of last.  Might need to grab range key and index key to build the right value...
        Map<String, AttributeValue> lastEvaluatedKey = getLastEvaluatedKey();

        if (lastEvaluatedKey == null) {
            // what if read partially onto the last 'page' and dynamo says no more stuff, but we may still want to
            // create a lastEvaluatedKey...  Do we need to hold on to one of these as a "template"?
            // TODO: is there a scenario where we could read and never terminate?
            return null;
        }

        Map<String, AttributeValue> generatedKey = new HashMap<String, AttributeValue>(lastEvaluatedKey.size());

        for (String key : lastEvaluatedKey.keySet()) {
            generatedKey.put(key, dynamoDBIterator.getLastItem().get(key));
        }

        return generatedKey;
    }


    private String encode(AttributeValue attributeValue)
            throws UnsupportedEncodingException {
        String intermediate;

        if (attributeValue.getS() != null) {
            intermediate = "S" + attributeValue.getS();
        } else if (attributeValue.getN() != null) {
            intermediate = "N" + attributeValue.getN();
        } else {
            throw new JeppettoException("Can only handle 'S' and 'N' scalar types: " + attributeValue);
        }

        return URLEncoder.encode(intermediate, StandardCharsets.UTF_8.name());
    }


    private AttributeValue decode(String encoded)
            throws UnsupportedEncodingException {
        if (encoded.startsWith("S")) {
            return new AttributeValue().withS(URLDecoder.decode(encoded.substring(1), StandardCharsets.UTF_8.name()));
        } else if (encoded.startsWith("N")) {
            return new AttributeValue().withN(URLDecoder.decode(encoded.substring(1), StandardCharsets.UTF_8.name()));
        } else {
            throw new JeppettoException("Can only handle 'S' and 'N' scalar types: " + encoded);
        }
    }


    //-------------------------------------------------------------
    // Inner Class - DynamoDBIterator
    //-------------------------------------------------------------

    class DynamoDBIterator implements Iterator<T> {

        //-------------------------------------------------------------
        // Variables - Private
        //-------------------------------------------------------------

        private Iterator<Map<String, AttributeValue>> iterator;
        private int remaining;
        private Map<String, AttributeValue> lastItem;


        //-------------------------------------------------------------
        // Constructors
        //-------------------------------------------------------------

        DynamoDBIterator(int limit) {
            this.iterator = fetchItems();
            this.remaining = limit;
        }


        //-------------------------------------------------------------
        // Implementation - Iterator
        //-------------------------------------------------------------

        @Override
        public boolean hasNext() {
            return remaining != 0 && hasNext0();
        }


        @Override
        public T next() {
            if (remaining == 0) {
                throw new NoSuchElementException("Limit for query was reached.");
            }

            remaining--;

            lastItem = iterator.next();

            T t = enhancer.newInstance();

            ((DynamoDBPersistable) t).__putAll(lastItem);
            ((DynamoDBPersistable) t).__markPersisted(dynamoDB.toString());

            return t;
        }


        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }


        //-------------------------------------------------------------
        // Methods - Private
        //-------------------------------------------------------------

        private boolean hasNext0() {
            if (iterator.hasNext()) {
                return true;
            }

            // No items in the current iterator.  If more items are available, fetch them and recheck the (new)
            // current iterator.  Continue until no more.
            while (moreAvailable()) {
                Map<String, AttributeValue> lastEvaluatedKey = getLastEvaluatedKey();

                iterator = fetchItems();

                if (iterator.hasNext()) {
                    setExclusiveStartKey(lastEvaluatedKey);

                    return true;
                }
            }

            return false;
        }


        private Map<String, AttributeValue> getLastItem() {
            return lastItem;
        }
    }
}
