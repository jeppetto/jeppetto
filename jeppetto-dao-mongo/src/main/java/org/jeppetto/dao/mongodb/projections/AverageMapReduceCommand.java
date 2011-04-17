/*
 * Copyright (c) 2011 Jeppetto and Jonathan Thompson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jeppetto.dao.mongodb.projections;


import com.mongodb.DBObject;

import java.math.BigDecimal;


/**
 * Map reduce job that computes an average value.
 */
class AverageMapReduceCommand
        extends MapReduceCommand {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private String field;


    //-------------------------------------------------------------
    // Constructor
    //-------------------------------------------------------------

    public AverageMapReduceCommand(DBObject query, String field) {
        super(query);

        this.field = field;
    }


    //-------------------------------------------------------------
    // Override - MapReduceCommand
    //-------------------------------------------------------------

    @Override
    protected Object defaultValue() {
        return 0d;
    }


    @Override
    protected String createMapFunction() {
        return String.format("function() { emit( 1, { total : this.%1$s , num : 1 } ); }", field);
    }


    @Override
    protected String createReduceFunction() {
        return "function(k,v) { " +
                "var n = { total : 0, num : 0 };" +
                "var count=0; " +
                "var sum=0; " +
                "for(var i in v) { " +
                "    n.total += v[i].total;" +
                "    n.num += v[i].num;" +
                "} " +
                "return n; " +
                "}";
    }


    @Override
    protected Double transformToValue(DBObject dbObject) {
        DBObject valueObject = (DBObject) dbObject.get("value");
        Number total = (Number) valueObject.get("total");
        Number number = (Number) valueObject.get("num");

        if (number.intValue() == 0) {
            return 0d;
        } else {
            return BigDecimal.valueOf(total.doubleValue()).divide(BigDecimal.valueOf(number.doubleValue())).doubleValue();
        }
    }
}
