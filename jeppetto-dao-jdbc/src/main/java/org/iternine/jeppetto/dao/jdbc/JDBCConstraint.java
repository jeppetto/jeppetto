/*
 * Copyright (c) 2011 Jeppetto and Jonathan Thompson
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


public class JDBCConstraint {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private String constraintString;
    private Object parameter1;
    private Object parameter2;


    //-------------------------------------------------------------
    // Methods - Getter/Setter
    //-------------------------------------------------------------

    public String getConstraintString() {
        return constraintString;
    }


    public void setConstraintString(String constraintString) {
        this.constraintString = constraintString;
    }


    public Object getParameter1() {
        return parameter1;
    }


    public void setParameter1(Object parameter1) {
        this.parameter1 = parameter1;
    }


    public Object getParameter2() {
        return parameter2;
    }


    public void setParameter2(Object parameter2) {
        this.parameter2 = parameter2;
    }
}
