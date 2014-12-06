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


import com.amazonaws.services.dynamodbv2.model.Condition;


/**
 */
public class DynamoDBConstraint {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private DynamoDBOperator operator;
    private Object[] values;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public DynamoDBConstraint(DynamoDBOperator operator, Object... values) {
        this.operator = operator;
        this.values = values;
    }


    //-------------------------------------------------------------
    // Methods - Public
    //-------------------------------------------------------------

    public Condition asCondition() {
        Condition condition = new Condition().withComparisonOperator(operator.getComparisonOperator());
        int argumentCount = operator.getArgumentCount();

        if (argumentCount == 1) {
            condition.withAttributeValueList(ConversionUtil.toAttributeValue(values[0]));
        } else if (argumentCount == 2) {
            condition.withAttributeValueList(ConversionUtil.toAttributeValue(values[0]), ConversionUtil.toAttributeValue(values[1]));
        } else if (argumentCount != 0) {    // N arguments
            condition.setAttributeValueList(ConversionUtil.toAttributeValueList(values[0]));
        }

        return condition;
    }


    //-------------------------------------------------------------
    // Methods - Getter
    //-------------------------------------------------------------

    public DynamoDBOperator getOperator() {
        return operator;
    }


    public Object[] getValues() {
        return values;
    }
}
