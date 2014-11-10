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

package org.iternine.jeppetto.dao.jdbc;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public enum JDBCCondition {

    //-------------------------------------------------------------
    // Enumeration Values
    //-------------------------------------------------------------

    // NB: Make sure that values are in an order to ensure that name matching (for DSL-style criteria)
    // captures the longest matching operator.  For example, 'fooNotEqual' should generate 'foo' and
    // 'NotEqual', not 'fooNot' and 'Equal'.
    NotEqual(" <> ? "),
    GreaterThanEqual(" >= ? "),
    LessThanEqual(" <= ? "),
    Equal(" = ? "),
    GreaterThan(" > ? "),
    LessThan(" < ? "),
    NotWithin(" NOT IN "),
    Within(" IN "),
    Between(" BETWEEN ? AND ? "),
    IsNull(" IS NULL "),
    IsNotNull(" IS NOT NULL ");


    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private String operator;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    JDBCCondition(String operator) {
        this.operator = operator;
    }


    //-------------------------------------------------------------
    // Methods - Public
    //-------------------------------------------------------------

    public Object buildConstraint(Iterator argsIterator) {
        JDBCConstraint jdbcConstraint = new JDBCConstraint();

        jdbcConstraint.setConstraintString(operator);

        switch (this) {
        case Equal:
        case NotEqual:
        case GreaterThan:
        case GreaterThanEqual:
        case LessThan:
        case LessThanEqual:
        case Within:    // TODO: Validate argument is a list or array?
        case NotWithin:
            jdbcConstraint.setParameter1(translateEnumsIfNecessary(argsIterator.next()));

            break;

        case Between:
            // TODO: doc note about inclusive/exclusive of value and order of values
            jdbcConstraint.setParameter1(translateEnumsIfNecessary(argsIterator.next()));
            jdbcConstraint.setParameter2(translateEnumsIfNecessary(argsIterator.next()));

            break;

        case IsNull:
        case IsNotNull:
            // No parameters

            break;

        default:
            throw new IllegalArgumentException("Unexpected enumeration: " + this);
        }

        return jdbcConstraint;
    }


    //-------------------------------------------------------------
    // Methods - Public
    //-------------------------------------------------------------

    private Object translateEnumsIfNecessary(Object argument) {
        if (argument == null) {
            return null;
        } else if (Enum.class.isAssignableFrom(argument.getClass())) {
            return ((Enum) argument).name();
        } else if (List.class.isAssignableFrom(argument.getClass())
                   && ((List) argument).size() > 0
                   && Enum.class.isAssignableFrom(((List) argument).get(0).getClass())) {
            List<String> newList = new ArrayList<String>();

            // Assume if the first item on the list is an enum, all are enums.
            for (Object listMember : (List) argument) {
                newList.add(((Enum) listMember).name());
            }

            return newList;
        } else {
            return argument;
        }
    }
}
