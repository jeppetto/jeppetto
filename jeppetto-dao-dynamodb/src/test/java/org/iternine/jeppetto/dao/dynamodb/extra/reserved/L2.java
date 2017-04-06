/*
 * Copyright (c) 2011-2017 Jeppetto and Jonathan Thompson
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

package org.iternine.jeppetto.dao.dynamodb.extra.reserved;


import java.util.Map;


public class L2 {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private String next;        // reserved name
    private Map<String, L3> l3Map;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public L2() {
    }


    public L2(String next, Map<String, L3> l3Map) {
        this.next = next;
        this.l3Map = l3Map;
    }


    //-------------------------------------------------------------
    // Methods - Getter/Setter
    //-------------------------------------------------------------

    public String getNext() {
        return next;
    }


    public void setNext(String next) {
        this.next = next;
    }


    public Map<String, L3> getL3Map() {
        return l3Map;
    }


    public void setL3Map(Map<String, L3> l3Map) {
        this.l3Map = l3Map;
    }
}
