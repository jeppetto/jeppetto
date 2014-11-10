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


import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;

import java.util.Iterator;


public enum DynamoDBOperator {

    //-------------------------------------------------------------
    // Enumeration Values
    //-------------------------------------------------------------

    NotEqual(ComparisonOperator.NE, 1),
    GreaterThanEqual(ComparisonOperator.GE, 1),
    LessThanEqual(ComparisonOperator.LE, 1),
    Equal(ComparisonOperator.EQ, 1),
    GreaterThan(ComparisonOperator.GT, 1),
    LessThan(ComparisonOperator.LT, 1),
    NotWithin(/* no equivalent ComparisonOperator, but can build a condition expression */ null, -1),
    Within(ComparisonOperator.IN, -1),
    Between(ComparisonOperator.BETWEEN, 2),
    IsNull(ComparisonOperator.NOT_NULL, 0),
    IsNotNull(ComparisonOperator.NULL, 0);


    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private ComparisonOperator comparisonOperator;
    private int argumentCount;


    private DynamoDBOperator(ComparisonOperator comparisonOperator, int argumentCount) {
        this.comparisonOperator = comparisonOperator;
        this.argumentCount = argumentCount;
    }


    //-------------------------------------------------------------
    // Methods - Public
    //-------------------------------------------------------------

    public DynamoDBConstraint buildConstraint(Iterator argsIterator) {
        Object[] values;

        if (argumentCount == 0) {
            values = null;
        } else if (argumentCount == 1) {
            values = new Object[] { argsIterator.next() };
        } else if (argumentCount == 2) {
            values = new Object[] { argsIterator.next(), argsIterator.next() };
        } else {    // N arguments
            values = new Object[] { argsIterator.next() };
        }

        return new DynamoDBConstraint(this, values);
    }


    public ComparisonOperator getComparisonOperator() {
        return comparisonOperator;
    }


    public int getArgumentCount() {
        return argumentCount;
    }
}