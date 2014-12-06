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


import org.iternine.jeppetto.dao.Condition;
import org.iternine.jeppetto.dao.JeppettoException;
import org.iternine.jeppetto.dao.QueryModel;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 */
public class ConditionExpressionBuilder {

    //-------------------------------------------------------------
    // Constants
    //-------------------------------------------------------------

    private static final Map<DynamoDBOperator, String> OPERATOR_EXPRESSIONS = new HashMap<DynamoDBOperator, String>(11) {{
        put(DynamoDBOperator.NotEqual, "%s <> :c%d");
        put(DynamoDBOperator.GreaterThanEqual, "%s >= :c%d");
        put(DynamoDBOperator.LessThanEqual, "%s <= :c%d");
        put(DynamoDBOperator.Equal, "%s = :c%d");
        put(DynamoDBOperator.GreaterThan, "%s > :c%d");
        put(DynamoDBOperator.LessThan, "%s < :c%d");
        put(DynamoDBOperator.NotWithin, "NOT %s IN %s");
        put(DynamoDBOperator.Within, "%s IN %s");
        put(DynamoDBOperator.Between, "%s BETWEEN :c%d AND :c%d");
        put(DynamoDBOperator.IsNull, "attribute_not_exists(%s)");
        put(DynamoDBOperator.IsNotNull, "attribute_exists(%s)");
    }};

    private static final Set<ComparisonOperator> RANGE_KEY_COMPARISON_OPERATORS = new HashSet<ComparisonOperator>() {{
        add(ComparisonOperator.EQ);
        add(ComparisonOperator.LE);
        add(ComparisonOperator.LT);
        add(ComparisonOperator.GE);
        add(ComparisonOperator.GT);
        add(ComparisonOperator.BEGINS_WITH);
        add(ComparisonOperator.BETWEEN);
    }};


    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private Condition hashKeyCondition;
    private Condition rangeKeyCondition;
    private StringBuilder expression = new StringBuilder();
    private Map<String, AttributeValue> attributeValues = new HashMap<String, AttributeValue>();
    private int attributeValueCounter = 0;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public ConditionExpressionBuilder() {
    }


    public ConditionExpressionBuilder(QueryModel queryModel, String hashKeyField, Map<String, String> localIndexes) {
        if (queryModel.getConditions() != null) {
            for (Condition condition : queryModel.getConditions()) {
                DynamoDBConstraint dynamoDBConstraint = (DynamoDBConstraint) condition.getConstraint();
                ComparisonOperator comparisonOperator = dynamoDBConstraint.getOperator().getComparisonOperator();

                if (condition.getField().equals(hashKeyField) && comparisonOperator == ComparisonOperator.EQ) {
                    this.hashKeyCondition = condition;
                } else if (rangeKeyCondition == null     // First one wins...
                           && localIndexes.containsKey(condition.getField())
                           && RANGE_KEY_COMPARISON_OPERATORS.contains(comparisonOperator)) {
                    this.rangeKeyCondition = condition;
                } else {
                    add(condition.getField(), dynamoDBConstraint);
                }
            }
        }

        if (queryModel.getAssociationConditions() != null) {
            for (Map.Entry<String, List<Condition>> associationConditions : queryModel.getAssociationConditions().entrySet()) {
                for (Condition condition : associationConditions.getValue()) {
                    add(associationConditions.getKey() + "." + condition.getField(), (DynamoDBConstraint) condition.getConstraint());
                }
            }
        }
    }


    //-------------------------------------------------------------
    // Methods - Package
    //-------------------------------------------------------------

    boolean hasHashKeyCondition() {
        return hashKeyCondition != null;
    }


    Map<String, com.amazonaws.services.dynamodbv2.model.Condition> getKeyConditions() {
        if (rangeKeyCondition == null) {
            return Collections.singletonMap(hashKeyCondition.getField(), ((DynamoDBConstraint) hashKeyCondition.getConstraint()).asCondition());
        } else {
            Map<String, com.amazonaws.services.dynamodbv2.model.Condition> keyConditions = new HashMap<String, com.amazonaws.services.dynamodbv2.model.Condition>();

            keyConditions.put(hashKeyCondition.getField(), ((DynamoDBConstraint) hashKeyCondition.getConstraint()).asCondition());
            keyConditions.put(rangeKeyCondition.getField(), ((DynamoDBConstraint) rangeKeyCondition.getConstraint()).asCondition());

            return keyConditions;
        }
    }


    String getRangeKey() {
        if (rangeKeyCondition == null) {
            return null;
        }

        return rangeKeyCondition.getField();
    }


    void convertRangeKeyConditionToExpression() {
        if (rangeKeyCondition == null) {
            return;
        }

        add(rangeKeyCondition.getField(), (DynamoDBConstraint) rangeKeyCondition.getConstraint());
    }


    Map<String, AttributeValue> getKey() {
        Map<String, AttributeValue> key;

        if (rangeKeyCondition == null) {
            key = Collections.singletonMap(hashKeyCondition.getField(), ConversionUtil.toAttributeValue(((DynamoDBConstraint) hashKeyCondition.getConstraint()).getValues()[0]));
        } else {
            key = new HashMap<String, AttributeValue>(2);

            key.put(hashKeyCondition.getField(), ConversionUtil.toAttributeValue(((DynamoDBConstraint) hashKeyCondition.getConstraint()).getValues()[0]));
            key.put(rangeKeyCondition.getField(), ConversionUtil.toAttributeValue(((DynamoDBConstraint) rangeKeyCondition.getConstraint()).getValues()[0]));
        }

        return key;
    }


    boolean hasExpression() {
        return expression.length() > 0;
    }


    String getExpression() {
        return expression.toString();
    }


    Map<String, AttributeValue> getAttributeValues() {
        return attributeValues;
    }


    ConditionExpressionBuilder with(String field, DynamoDBConstraint constraint) {
        add(field, constraint);

        return this;
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private void add(String field, DynamoDBConstraint constraint) {
        if (expression.length() > 0) {
            expression.append(" and ");
        }

        expression.append(buildCondition(field, constraint));

        Map<String, AttributeValue> newValues = getExpressionAttributeValues(constraint);
        attributeValues.putAll(newValues);
    }


    private String buildCondition(String attribute, DynamoDBConstraint constraint) {
        int argumentCount = constraint.getOperator().getArgumentCount();
        String operatorExpression = OPERATOR_EXPRESSIONS.get(constraint.getOperator());

        if (argumentCount == 0) {
            return String.format(operatorExpression, attribute);
        } else if (argumentCount == 1) {
            return String.format(operatorExpression, attribute, attributeValueCounter);
        } else if (argumentCount == 2) {
            return String.format(operatorExpression, attribute, attributeValueCounter, attributeValueCounter + 1);
        } else {    // N arguments
            StringBuilder placeholders = new StringBuilder("(");
            int itemCount = getItemCount(constraint.getValues()[0]);

            for (int i = 0; i < itemCount; i++) {
                if (i > 0) {
                    placeholders.append(", ");
                }

                placeholders.append(":c").append(attributeValueCounter + i);
            }

            placeholders.append(')');

            return String.format(operatorExpression, attribute, placeholders.toString());
        }
    }


    private Map<String, AttributeValue> getExpressionAttributeValues(DynamoDBConstraint constraint) {
        int argumentCount = constraint.getOperator().getArgumentCount();
        Object[] values = constraint.getValues();

        if (argumentCount == 0) {
            return Collections.emptyMap();
        } else if (argumentCount == 1) {
            return Collections.singletonMap(":c" + attributeValueCounter++, ConversionUtil.toAttributeValue(values[0]));
        } else if (argumentCount == 2) {
            Map<String, AttributeValue> result = new HashMap<String, AttributeValue>(2);

            result.put(":c" + attributeValueCounter++, ConversionUtil.toAttributeValue(values[0]));
            result.put(":c" + (attributeValueCounter++), ConversionUtil.toAttributeValue(values[1]));

            return result;
        } else {    // N arguments
            Collection<AttributeValue> attributeValues = ConversionUtil.toAttributeValueList(values[0]);
            Map<String, AttributeValue> result = new HashMap<String, AttributeValue>(attributeValues.size());

            for (AttributeValue attributeValue : attributeValues) {
                result.put(":c" + attributeValueCounter++, attributeValue);
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
