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

package org.iternine.jeppetto.dao.dynamodb;


import org.iternine.jeppetto.dao.JeppettoException;
import org.iternine.jeppetto.dao.dirtyable.DirtyableList;
import org.iternine.jeppetto.dao.dirtyable.DirtyableMap;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;


/**
 */
public class UpdateExpressionBuilder {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private StringBuilder setExpression = new StringBuilder();
    private StringBuilder removeExpression = new StringBuilder();
    private Map<String, AttributeValue> attributeValues = new LinkedHashMap<String, AttributeValue>();
    private int attributeValueCounter = 0;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public UpdateExpressionBuilder(DynamoDBPersistable dynamoDBPersistable) {
        extractUpdateDetails(dynamoDBPersistable, "");
    }


    //-------------------------------------------------------------
    // Methods - Getter/Setter
    //-------------------------------------------------------------

    public String getExpression() {
        StringBuilder expression = new StringBuilder();

        if (setExpression.length() > 0) {
            expression.append("SET ");
            expression.append(setExpression);
        }

        if (removeExpression.length() > 0) {
            if (expression.length() > 0) {
                expression.append(' ');
            }

            expression.append("REMOVE ");
            expression.append(removeExpression);
        }

        return expression.toString();
    }


    public Map<String, AttributeValue> getAttributeValues() {
        return attributeValues;
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private void extractUpdateDetails(DynamoDBPersistable dynamoDBPersistable, String prefix) {
        for (Iterator<String> dirtyKeysIterator = dynamoDBPersistable.getDirtyFields(); dirtyKeysIterator.hasNext(); ) {
            String key = dirtyKeysIterator.next();
            Object object = dynamoDBPersistable.get(key);
            String fullyQualifiedKey = prefix + key;

            if (object == null) {
                append(removeExpression, fullyQualifiedKey);
            } else if (DynamoDBPersistable.class.isAssignableFrom(object.getClass())) {
                extractUpdateDetails((DynamoDBPersistable) object, fullyQualifiedKey + ".");
            } else if (DirtyableList.class.isAssignableFrom(object.getClass())) {
                // TODO: implement
                throw new JeppettoException("Not yet implemented");
            } else if (DirtyableMap.class.isAssignableFrom(object.getClass())) {
                // TODO: implement
                throw new JeppettoException("Not yet implemented");
            } else {
                append(setExpression, fullyQualifiedKey + " = :a" + attributeValueCounter);
                attributeValues.put(":a" + attributeValueCounter, ConversionUtil.toAttributeValue(object));

                attributeValueCounter++;
            }
        }
    }


    private void append(StringBuilder sb, String text) {
        if (sb.length() > 0) {
            sb.append(", ");
        }

        sb.append(text);
    }
}
