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


import org.iternine.jeppetto.dao.dynamodb.ConversionUtil;
import org.iternine.jeppetto.dao.dynamodb.DynamoDBPersistable;
import org.iternine.jeppetto.dao.persistable.PersistableList;
import org.iternine.jeppetto.dao.persistable.PersistableMap;
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

    private static final String EXPRESSION_ATTRIBUTE_VALUE_PREFIX = ":u";
    private static final String EXPRESSION_ATTRIBUTE_NAME_PREFIX = "#u";


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
    public boolean hasExpression() {
        return setExpression.length() > 0 || removeExpression.length() > 0;
    }


    @Override
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


    @Override
    public String getExpressionAttributeValuePrefix() {
        return EXPRESSION_ATTRIBUTE_VALUE_PREFIX;
    }


    @Override
    public String getExpressionAttributeNamePrefix() {
        return EXPRESSION_ATTRIBUTE_NAME_PREFIX;
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private void extractUpdateDetails(DynamoDBPersistable dynamoDBPersistable, String prefix) {
        for (Iterator<String> dirtyFields = dynamoDBPersistable.__getDirtyFields(); dirtyFields.hasNext(); ) {
            String dirtyField = dirtyFields.next();

            processDirtyObject(dynamoDBPersistable.__get(dirtyField), prefix + getExpressionAttributeName(dirtyField));
        }
    }


    private void extractUpdateDetails(PersistableMap persistableMap, String prefix) {
        for (Iterator<String> dirtyFields = persistableMap.__getDirtyFields(); dirtyFields.hasNext(); ) {
            String dirtyField = dirtyFields.next();

            processDirtyObject(persistableMap.get(dirtyField), prefix + getExpressionAttributeName(dirtyField));
        }
    }


    private void extractUpdateDetails(PersistableList persistableList, String prefix) {
        if (persistableList.isRewrite()) {
            addToSetExpression(persistableList, prefix);

            return;
        }

        for (Iterator<String> dirtyIndexes = persistableList.__getDirtyFields(); dirtyIndexes.hasNext(); ) {
            int dirtyIndex = Integer.parseInt(dirtyIndexes.next());

            processDirtyObject(persistableList.get(dirtyIndex), prefix +'[' + dirtyIndex + ']');
        }
    }


    private void processDirtyObject(Object dirtyObject, String fullyQualifiedField) {
        if (dirtyObject == null) {
            append(removeExpression, fullyQualifiedField);
        } else if (PersistableList.class.isAssignableFrom(dirtyObject.getClass())) {
            extractUpdateDetails((PersistableList) dirtyObject, fullyQualifiedField);
        } else if (PersistableMap.class.isAssignableFrom(dirtyObject.getClass())) {
            extractUpdateDetails((PersistableMap) dirtyObject, fullyQualifiedField + ".");
        } else if (DynamoDBPersistable.class.isAssignableFrom(dirtyObject.getClass())) {
            extractUpdateDetails((DynamoDBPersistable) dirtyObject, fullyQualifiedField + ".");
        } else {
            addToSetExpression(dirtyObject, fullyQualifiedField);
        }
    }


    private void extractUpdateDetails(UpdateObject updateObject, String prefix) {
        for (Map.Entry<String, Object> updateEntry : updateObject.__getUpdates().entrySet()) {
            String fullyQualifiedField = prefix + getExpressionAttributeName(updateEntry.getKey());
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
        String expressionAttributeValueKey = putExpressionAttributeValue(ConversionUtil.toAttributeValue(object));

        append(setExpression, fullyQualifiedField + " = " + expressionAttributeValueKey);
    }


    private void addListItemsToSetExpression(List<Object> adds, String fullyQualifiedField) {
        String expressionAttributeValueKey = putExpressionAttributeValue(ConversionUtil.toAttributeValue(adds));

        append(setExpression, fullyQualifiedField + " = list_append(" + fullyQualifiedField + ", " + expressionAttributeValueKey + ')');
    }


    private void addIncrementToSetExpression(Number number, String fullyQualifiedField) {
        String numberString = number.toString();
        String incrementString;

        if (numberString.charAt(0) == '-') {    // is negative
            String expressionAttributeValueKey = putExpressionAttributeValue(new AttributeValue().withN(numberString.substring(1)));

            incrementString = " - " + expressionAttributeValueKey;
        } else {                                // is positive
            String expressionAttributeValueKey = putExpressionAttributeValue(new AttributeValue().withN(numberString));

            incrementString = " + " + expressionAttributeValueKey;
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
