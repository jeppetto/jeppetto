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

package org.iternine.jeppetto.dao.mongodb;


import com.mongodb.BasicDBObject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public enum MongoDBOperator {

    //-------------------------------------------------------------
    // Enumeration Values
    //-------------------------------------------------------------

    // NB: Make sure that values are in an order to ensure that name matching (for DSL-style criteria)
    // captures the longest matching operator.  For example, 'fooNotEqual' should generate 'foo' and
    // 'NotEqual', not 'fooNot' and 'Equal'.
    NotEqual("$ne"),
    GreaterThanEqual("$gte"),
    LessThanEqual("$lte"),
    Equal(null),
    GreaterThan("$gt"),
    LessThan("$lt"),
    NotWithin("$nin"),
    Within("$in"),
    Between(null),
    IsNull(null),
    IsNotNull(null),
    BeginsWith(null),
    ElementMatches("$elemMatch");


    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private String operator;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    MongoDBOperator(String operator) {
        this.operator = operator;
    }


    //-------------------------------------------------------------
    // Methods - Public
    //-------------------------------------------------------------

    public Object buildConstraint(Iterator argsIterator) {
        switch (this) {
        case Equal:
            return translateEnumsIfNecessary(argsIterator.next());

        case NotEqual:
        case GreaterThan:
        case GreaterThanEqual:
        case LessThan:
        case LessThanEqual:
        case Within:    // TODO: Validate argument is a list or array?
        case NotWithin:
        case ElementMatches:
            return new BasicDBObject(operator, translateEnumsIfNecessary(argsIterator.next()));

        case Between:
            // TODO: doc note about inclusive/exclusive of value and order of values
            BasicDBObject betweenConstraint = new BasicDBObject();

            betweenConstraint.put(GreaterThan.operator, translateEnumsIfNecessary(argsIterator.next()));
            betweenConstraint.put(LessThan.operator, translateEnumsIfNecessary(argsIterator.next()));

            return betweenConstraint;

        case IsNull:
            BasicDBObject nullConstraint = new BasicDBObject();

            nullConstraint.put("$exists", Boolean.TRUE);
            nullConstraint.put("$type", 10);

            return nullConstraint;

        case IsNotNull:
            BasicDBObject notNullConstraint = new BasicDBObject();

            notNullConstraint.put("$exists", Boolean.TRUE);
            notNullConstraint.put("$not", new BasicDBObject("$type", 10));

            return notNullConstraint;

        case BeginsWith:
            // TODO: Escape argsIterator.next()
            return new BasicDBObject("$regex", "^" + argsIterator.next() + ".*");

        default:
            throw new IllegalArgumentException("Unexpected enumeration: " + this);
        }
    }


    //-------------------------------------------------------------
    // Methods - Private
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
