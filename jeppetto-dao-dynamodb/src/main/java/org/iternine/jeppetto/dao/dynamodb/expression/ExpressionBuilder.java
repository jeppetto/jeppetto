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

package org.iternine.jeppetto.dao.dynamodb.expression;


import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public abstract class ExpressionBuilder {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private Map<String, AttributeValue> expressionAttributeValues;
    private int expressionAttributeValueCounter = 0;
//    private int attributeNameCounter = 0;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    protected ExpressionBuilder(boolean expressionAttributesExpected) {
        expressionAttributeValues = expressionAttributesExpected ? new HashMap<String, AttributeValue>() : Collections.<String, AttributeValue>emptyMap();
    }


    //-------------------------------------------------------------
    // Methods - Abstract
    //-------------------------------------------------------------

    abstract public boolean hasExpression();

    abstract public String getExpression();

    abstract public String getExpressionAttributePrefix();


    //-------------------------------------------------------------
    // Methods - Protected
    //-------------------------------------------------------------

    protected String putExpressionAttributeValue(AttributeValue expressionAttributeValue) {
        String expressionAttributeKey = getExpressionAttributePrefix() + expressionAttributeValueCounter++;

        expressionAttributeValues.put(expressionAttributeKey, expressionAttributeValue);

        return expressionAttributeKey;
    }


    //-------------------------------------------------------------
    // Methods - Getters
    //-------------------------------------------------------------

    public Map<String, AttributeValue> getExpressionAttributeValues() {
        return expressionAttributeValues;
    }


    // Map<String, String> getExpressionAttributeNames();
}
