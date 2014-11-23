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
import org.iternine.jeppetto.dao.persistable.PersistableList;
import org.iternine.jeppetto.dao.updateobject.NumericIncrement;
import org.iternine.jeppetto.dao.updateobject.UpdateList;
import org.iternine.jeppetto.dao.updateobject.UpdateMap;
import org.iternine.jeppetto.dao.updateobject.UpdateObject;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
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


    public UpdateExpressionBuilder(UpdateObject updateObject) {
        extractUpdateDetails(updateObject, "");
    }


    //-------------------------------------------------------------
    // Methods - Package
    //-------------------------------------------------------------

    String getExpression() {
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


    Map<String, AttributeValue> getAttributeValues() {
        return attributeValues;
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private void extractUpdateDetails(DynamoDBPersistable dynamoDBPersistable, String prefix) {
        for (Iterator<String> dirtyFieldsIterator = dynamoDBPersistable.__getDirtyFields(); dirtyFieldsIterator.hasNext(); ) {
            String field = dirtyFieldsIterator.next();
            Object object = dynamoDBPersistable.__get(field);
            String fullyQualifiedField = prefix + field;

            if (object == null) {
                append(removeExpression, fullyQualifiedField);
            } else if (PersistableList.class.isAssignableFrom(object.getClass())) {
                // TODO: implement
                throw new JeppettoException("Not yet implemented");
            } else if (DynamoDBPersistable.class.isAssignableFrom(object.getClass())) {     // Includes PersistableMap as well
                extractUpdateDetails((DynamoDBPersistable) object, fullyQualifiedField + ".");
            } else {
                addToSetExpression(fullyQualifiedField, object);
            }
        }
    }


    private void extractUpdateDetails(UpdateObject updateObject, String prefix) {
        for (Map.Entry<String, Object> updateEntry : updateObject.__getUpdates().entrySet()) {
            String fullyQualifiedField = prefix + updateEntry.getKey();
            Object object = updateEntry.getValue();

            if (object == null) {
                append(removeExpression, fullyQualifiedField);
            } else if (UpdateList.class.isAssignableFrom(object.getClass())) {
                UpdateList updateList = (UpdateList) object;

                if (updateList.wasCleared()) {
                    addToSetExpression(fullyQualifiedField, new ArrayList<Object>(updateList.__getUpdates().values()));
                } else {
                    extractUpdateDetails(updateList, fullyQualifiedField);
                }
            } else if (UpdateMap.class.isAssignableFrom(object.getClass())) {
                UpdateMap updateMap = (UpdateMap) object;

                if (updateMap.wasCleared()) {
                    addToSetExpression(fullyQualifiedField, updateMap);
                } else {
                    extractUpdateDetails(updateMap, fullyQualifiedField + ".");
                }
            } else if (NumericIncrement.class.isAssignableFrom(object.getClass())) {
                addIncrementToSetExpression(fullyQualifiedField, ((NumericIncrement) object).getIncrement());
            } else if (UpdateObject.class.isAssignableFrom(object.getClass())) {
                extractUpdateDetails((UpdateObject) object, fullyQualifiedField + ".");
            } else {
                addToSetExpression(fullyQualifiedField, object);
            }
        }
    }


    private void addToSetExpression(String fullyQualifiedField, Object object) {
        append(setExpression, fullyQualifiedField + " = :a" + attributeValueCounter);
        attributeValues.put(":a" + attributeValueCounter, ConversionUtil.toAttributeValue(object));

        attributeValueCounter++;
    }


    private void addIncrementToSetExpression(String fullyQualifiedField, Number number) {
        String numberString = number.toString();
        String incrementString;

        if (numberString.charAt(0) == '-') {    // is negative
            incrementString = " - :a" + attributeValueCounter;

            attributeValues.put(":a" + attributeValueCounter, new AttributeValue().withN(numberString.substring(1)));
        } else {                                // is positive
            incrementString = " + :a" + attributeValueCounter;

            attributeValues.put(":a" + attributeValueCounter, new AttributeValue().withN(numberString));
        }

        append(setExpression, fullyQualifiedField + " = " + fullyQualifiedField + incrementString);

        attributeValueCounter++;
    }


//    private void addListToSetExpression(String fullyQualifiedField, List<Object> list) {
//        append(setExpression, fullyQualifiedField + " = list_append(" + fullyQualifiedField + ", :a" + attributeValueCounter + ')');
//        attributeValues.put(":a" + attributeValueCounter, ConversionUtil.toAttributeValue(list));
//
//        attributeValueCounter++;
//    }


    private void append(StringBuilder sb, String text) {
        if (sb.length() > 0) {
            sb.append(", ");
        }

        sb.append(text);
    }
}
