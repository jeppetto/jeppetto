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

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


/**
 */
public class ConditionExpressionBuilder {

    //-------------------------------------------------------------
    // Constants
    //-------------------------------------------------------------

    private static final Map<DynamoDBOperator, String> OPERATOR_EXPRESSIONS = new HashMap<DynamoDBOperator, String>(11) {{
        put(DynamoDBOperator.NotEqual, "%s <> :a%d");
        put(DynamoDBOperator.GreaterThanEqual, "%s >= :a%d");
        put(DynamoDBOperator.LessThanEqual, "%s <= :a%d");
        put(DynamoDBOperator.Equal, "%s = :a%d");
        put(DynamoDBOperator.GreaterThan, "%s > :a%d");
        put(DynamoDBOperator.LessThan, "%s < :a%d");
        put(DynamoDBOperator.NotWithin, "NOT %s IN %s");
        put(DynamoDBOperator.Within, "%s IN %s");
        put(DynamoDBOperator.Between, "%s BETWEEN :a%d AND :a%d");
        put(DynamoDBOperator.IsNull, "attribute_not_exists(%s)");
        put(DynamoDBOperator.IsNotNull, "attribute_exists(%s)");
    }};


    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private StringBuilder expression = new StringBuilder();
    private int placeholderCount = 0;
    private Map<String, AttributeValue> attributeValues = new HashMap<String, AttributeValue>();


    //-------------------------------------------------------------
    // Methods - Public
    //-------------------------------------------------------------

    public void add(String key, DynamoDBConstraint constraint) {
        if (expression.length() > 0) {
            expression.append(" and ");
        }

        expression.append(buildCondition(key, constraint, placeholderCount));

        Map<String, AttributeValue> newValues = getExpressionAttributeValues(constraint, placeholderCount);
        attributeValues.putAll(newValues);

        placeholderCount += newValues.size();
    }


    //-------------------------------------------------------------
    // Methods - Getter/Setter
    //-------------------------------------------------------------

    public boolean hasConditions() {
        return expression.length() > 0;
    }


    public String getExpression() {
        return expression.toString();
    }


    public Map<String, AttributeValue> getAttributeValues() {
        return attributeValues;
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private String buildCondition(String attribute, DynamoDBConstraint constraint, int placeholderCount) {
        int argumentCount = constraint.getOperator().getArgumentCount();
        String operatorExpression = OPERATOR_EXPRESSIONS.get(constraint.getOperator());

        if (argumentCount == 0) {
            return String.format(operatorExpression, attribute);
        } else if (argumentCount == 1) {
            return String.format(operatorExpression, attribute, placeholderCount);
        } else if (argumentCount == 2) {
            return String.format(operatorExpression, attribute, placeholderCount, placeholderCount + 1);
        } else {    // N arguments
            StringBuilder placeholders = new StringBuilder("(");
            int itemCount = getItemCount(constraint.getValues()[0]);

            for (int i = 0; i < itemCount; i++) {
                if (i > 0) {
                    placeholders.append(", ");
                }

                placeholders.append(":a").append(placeholderCount + i);
            }

            placeholders.append(')');

            return String.format(operatorExpression, attribute, placeholders.toString());
        }
    }


    private Map<String, AttributeValue> getExpressionAttributeValues(DynamoDBConstraint constraint, int placeholderCount) {
        int argumentCount = constraint.getOperator().getArgumentCount();
        Object[] values = constraint.getValues();

        if (argumentCount == 0) {
            return Collections.emptyMap();
        } else if (argumentCount == 1) {
            return Collections.singletonMap(":a" + placeholderCount, ConversionUtil.toAttributeValue(values[0]));
        } else if (argumentCount == 2) {
            Map<String, AttributeValue> result = new HashMap<String, AttributeValue>(2);

            result.put(":a" + placeholderCount, ConversionUtil.toAttributeValue(values[0]));
            result.put(":a" + (placeholderCount + 1), ConversionUtil.toAttributeValue(values[1]));

            return result;
        } else {    // N arguments
            Collection<AttributeValue> attributeValues = ConversionUtil.toAttributeValueList(values[0]);
            Map<String, AttributeValue> result = new HashMap<String, AttributeValue>(attributeValues.size());

            for (AttributeValue attributeValue : attributeValues) {
                result.put(":a" + placeholderCount++, attributeValue);
            }

            return result;
        }
    }


    private int getItemCount(Object value) {
        if (value.getClass().isArray()) {
            return Array.getLength(value);
        } else if (Collection.class.isAssignableFrom(value.getClass())) {
            return ((Collection) value).size();
        } else {
            throw new JeppettoException("Expected either array or Collection object.");
        }
    }
}
