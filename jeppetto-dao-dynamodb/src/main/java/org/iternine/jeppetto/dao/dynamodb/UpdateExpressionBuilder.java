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

import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 */
public class UpdateExpressionBuilder extends ExpressionBuilder {

    //-------------------------------------------------------------
    // Constants
    //-------------------------------------------------------------

    private static final String EXPRESSION_ATTRIBUTE_KEY_PREFIX = ":u";


    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private final StringBuilder setExpression = new StringBuilder();
    private final StringBuilder removeExpression = new StringBuilder();


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public UpdateExpressionBuilder(DynamoDBPersistable dynamoDBPersistable) {
        super(true);

        extractUpdateDetails(dynamoDBPersistable, "");
    }


    public UpdateExpressionBuilder(UpdateObject updateObject) {
        super(true);

        extractUpdateDetails(updateObject, "");
    }


    //-------------------------------------------------------------
    // Implementation - ExpressionBuilder
    //-------------------------------------------------------------

    @Override
    boolean hasExpression() {
        return setExpression.length() > 0 || removeExpression.length() > 0;
    }


    @Override
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


    @Override
    String getExpressionAttributePrefix() {
        return EXPRESSION_ATTRIBUTE_KEY_PREFIX;
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
                addToSetExpression(object, fullyQualifiedField);
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
                    addToSetExpression(updateList.getAdds(), fullyQualifiedField);
                } else {
                    // TODO: Can't actually have both index updates and list_appends.
                    extractUpdateDetails(updateList, fullyQualifiedField);

                    if (!updateList.getAdds().isEmpty()) {
                        addListItemsToSetExpression(updateList.getAdds(), fullyQualifiedField);
                    }
                }
            } else if (UpdateMap.class.isAssignableFrom(object.getClass())) {
                UpdateMap updateMap = (UpdateMap) object;

                if (updateMap.wasCleared()) {
                    addToSetExpression(updateMap, fullyQualifiedField);
                } else {
                    extractUpdateDetails(updateMap, fullyQualifiedField + ".");
                }
            } else if (NumericIncrement.class.isAssignableFrom(object.getClass())) {
                addIncrementToSetExpression(((NumericIncrement) object).getIncrement(), fullyQualifiedField);
            } else if (UpdateObject.class.isAssignableFrom(object.getClass())) {
                extractUpdateDetails((UpdateObject) object, fullyQualifiedField + ".");
            } else {
                addToSetExpression(object, fullyQualifiedField);
            }
        }
    }


    private void addToSetExpression(Object object, String fullyQualifiedField) {
        String expressionAttributeKey = putExpressionAttributeValue(ConversionUtil.toAttributeValue(object));

        append(setExpression, fullyQualifiedField + " = " + expressionAttributeKey);
    }


    private void addListItemsToSetExpression(List<Object> adds, String fullyQualifiedField) {
        String expressionAttributeKey = putExpressionAttributeValue(ConversionUtil.toAttributeValue(adds));

        append(setExpression, fullyQualifiedField + " = list_append(" + fullyQualifiedField + ", " + expressionAttributeKey + ')');
    }


    private void addIncrementToSetExpression(Number number, String fullyQualifiedField) {
        String numberString = number.toString();
        String incrementString;

        if (numberString.charAt(0) == '-') {    // is negative
            String expressionAttributeKey = putExpressionAttributeValue(new AttributeValue().withN(numberString.substring(1)));

            incrementString = " - " + expressionAttributeKey;
        } else {                                // is positive
            String expressionAttributeKey = putExpressionAttributeValue(new AttributeValue().withN(numberString));

            incrementString = " + " + expressionAttributeKey;
        }

        append(setExpression, fullyQualifiedField + " = " + fullyQualifiedField + incrementString);
    }


    private void append(StringBuilder sb, String text) {
        if (sb.length() > 0) {
            sb.append(", ");
        }

        sb.append(text);
    }
}
