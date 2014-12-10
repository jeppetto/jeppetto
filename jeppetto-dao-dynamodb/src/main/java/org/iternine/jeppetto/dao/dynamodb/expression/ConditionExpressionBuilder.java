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


import org.iternine.jeppetto.dao.Condition;
import org.iternine.jeppetto.dao.QueryModel;
import org.iternine.jeppetto.dao.dynamodb.ConversionUtil;
import org.iternine.jeppetto.dao.dynamodb.DynamoDBConstraint;
import org.iternine.jeppetto.dao.dynamodb.DynamoDBOperator;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 */
public class ConditionExpressionBuilder extends ExpressionBuilder {

    //-------------------------------------------------------------
    // Constants
    //-------------------------------------------------------------

    private static final Map<DynamoDBOperator, String> OPERATOR_EXPRESSIONS = new HashMap<DynamoDBOperator, String>(11) {{
        put(DynamoDBOperator.NotEqual, "%s <> %s");
        put(DynamoDBOperator.GreaterThanEqual, "%s >= %s");
        put(DynamoDBOperator.LessThanEqual, "%s <= %s");
        put(DynamoDBOperator.Equal, "%s = %s");
        put(DynamoDBOperator.GreaterThan, "%s > %s");
        put(DynamoDBOperator.LessThan, "%s < %s");
        put(DynamoDBOperator.NotWithin, "NOT %s IN %s");
        put(DynamoDBOperator.Within, "%s IN %s");
        put(DynamoDBOperator.Between, "%s BETWEEN %s AND %s");
        put(DynamoDBOperator.IsNull, "attribute_not_exists(%s)");
        put(DynamoDBOperator.IsNotNull, "attribute_exists(%s)");
    }};

    private static final Set<ComparisonOperator> RANGE_KEY_COMPARISON_OPERATORS = new HashSet<ComparisonOperator>(7) {{
        add(ComparisonOperator.EQ);
        add(ComparisonOperator.LE);
        add(ComparisonOperator.LT);
        add(ComparisonOperator.GE);
        add(ComparisonOperator.GT);
        add(ComparisonOperator.BEGINS_WITH);
        add(ComparisonOperator.BETWEEN);
    }};

    private static final String EXPRESSION_ATTRIBUTE_VALUE_PREFIX = ":c";
    private static final String EXPRESSION_ATTRIBUTE_NAME_PREFIX = "#c";


    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private Condition hashKeyCondition;
    private Condition rangeKeyCondition;
    private final StringBuilder expression = new StringBuilder();


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public ConditionExpressionBuilder() {
        super(true);
    }


    public ConditionExpressionBuilder(QueryModel queryModel, Map<String, Map<String, String>> indexes) {
        super(true);

        if (queryModel.getConditions() != null) {
            for (Condition condition : queryModel.getConditions()) {
                DynamoDBConstraint dynamoDBConstraint = (DynamoDBConstraint) condition.getConstraint();
                ComparisonOperator comparisonOperator = dynamoDBConstraint.getOperator().getComparisonOperator();

                if (indexes.containsKey(condition.getField()) && comparisonOperator == ComparisonOperator.EQ) {
                    this.hashKeyCondition = condition;
                } else if (hashKeyCondition != null
                           && rangeKeyCondition == null     // First one wins...
                           && indexes.get(hashKeyCondition.getField()).containsKey(condition.getField())
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
    // Implementation - ExpressionBuilder
    //-------------------------------------------------------------

    @Override
    public boolean hasExpression() {
        return expression.length() > 0;
    }


    @Override
    public String getExpression() {
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
    // Methods - Public
    //-------------------------------------------------------------

    public boolean hasHashKeyCondition() {
        return hashKeyCondition != null;
    }


    public Map<String, com.amazonaws.services.dynamodbv2.model.Condition> getKeyConditions() {
        if (rangeKeyCondition == null) {
            return Collections.singletonMap(hashKeyCondition.getField(), ((DynamoDBConstraint) hashKeyCondition.getConstraint()).asCondition());
        } else {
            Map<String, com.amazonaws.services.dynamodbv2.model.Condition> keyConditions = new HashMap<String, com.amazonaws.services.dynamodbv2.model.Condition>();

            keyConditions.put(hashKeyCondition.getField(), ((DynamoDBConstraint) hashKeyCondition.getConstraint()).asCondition());
            keyConditions.put(rangeKeyCondition.getField(), ((DynamoDBConstraint) rangeKeyCondition.getConstraint()).asCondition());

            return keyConditions;
        }
    }


    public String getHashKey() {
        if (hashKeyCondition == null) {
            return null;
        }

        return hashKeyCondition.getField();
    }


    public String getRangeKey() {
        if (rangeKeyCondition == null) {
            return null;
        }

        return rangeKeyCondition.getField();
    }


    public void convertRangeKeyConditionToExpression() {
        if (rangeKeyCondition == null) {
            return;
        }

        add(rangeKeyCondition.getField(), (DynamoDBConstraint) rangeKeyCondition.getConstraint());
    }


    public Map<String, AttributeValue> getKey() {
        Map<String, AttributeValue> key;

        if (rangeKeyCondition == null) {
            key = Collections.singletonMap(hashKeyCondition.getField(), ConversionUtil
                    .toAttributeValue(((DynamoDBConstraint) hashKeyCondition.getConstraint()).getValues()[0]));
        } else {
            key = new HashMap<String, AttributeValue>(2);

            key.put(hashKeyCondition.getField(), ConversionUtil.toAttributeValue(((DynamoDBConstraint) hashKeyCondition.getConstraint()).getValues()[0]));
            key.put(rangeKeyCondition.getField(), ConversionUtil.toAttributeValue(((DynamoDBConstraint) rangeKeyCondition.getConstraint()).getValues()[0]));
        }

        return key;
    }


    public ConditionExpressionBuilder with(String field, DynamoDBConstraint constraint) {
        add(field, constraint);

        return this;
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private void add(String attribute, DynamoDBConstraint constraint) {
        if (expression.length() > 0) {
            expression.append(" and ");
        }

        String expressionAttributeName = getExpressionAttributeName(attribute);
        String operatorExpression = OPERATOR_EXPRESSIONS.get(constraint.getOperator());
        int argumentCount = constraint.getOperator().getArgumentCount();
        Object[] values = constraint.getValues();

        if (argumentCount == 0) {
            expression.append(String.format(operatorExpression, expressionAttributeName));
        } else if (argumentCount == 1) {
            String expressionAttributeKey = putExpressionAttributeValue(ConversionUtil.toAttributeValue(values[0]));

            expression.append(String.format(operatorExpression, expressionAttributeName, expressionAttributeKey));
        } else if (argumentCount == 2) {
            String expressionAttributeKey0 = putExpressionAttributeValue(ConversionUtil.toAttributeValue(values[0]));
            String expressionAttributeKey1 = putExpressionAttributeValue(ConversionUtil.toAttributeValue(values[1]));

            expression.append(String.format(operatorExpression, expressionAttributeName, expressionAttributeKey0, expressionAttributeKey1));
        } else {    // N arguments
            StringBuilder placeholders = new StringBuilder("(");
            Collection<AttributeValue> attributeValues = ConversionUtil.toAttributeValueList(values[0]);

            for (AttributeValue attributeValue : attributeValues) {
                if (placeholders.length() > 1) {
                    placeholders.append(", ");
                }

                String expressionAttributeKey = putExpressionAttributeValue(attributeValue);

                placeholders.append(expressionAttributeKey);
            }

            placeholders.append(')');

            expression.append(String.format(operatorExpression, expressionAttributeName, placeholders.toString()));
        }
    }
}
