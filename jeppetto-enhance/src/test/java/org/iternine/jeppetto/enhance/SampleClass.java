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

package org.iternine.jeppetto.enhance;


import java.util.List;
import java.util.Map;


public class SampleClass {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private String foo;
    private List<String> baz;
    private long number;
    private Long numberObject;
    private boolean[] bits = new boolean[] { true, false, true, true, false };
    private Map<String, String> map;


    //-------------------------------------------------------------
    // Methods - Getter/Setter
    //-------------------------------------------------------------

    public String getFoo() {
        return foo;
    }


    public void setFoo(String foo) {
        this.foo = foo;
    }


    public List<String> getBaz() {
        return baz;
    }


    public void setBaz(List<String> baz) {
        this.baz = baz;
    }


    public long getNumber() {
        return number;
    }


    public void setNumber(long number) {
        this.number = number;
    }


    public Long getNumberObject() {
        return numberObject;
    }


    public void setNumberObject(Long numberObject) {
        this.numberObject = numberObject;
    }


    public boolean[] getBits() {
        return bits;
    }


    public void setBits(boolean[] bits) {
        this.bits = bits;
    }


    public Map<String, String> getMap() {
        return map;
    }


    public void setMap(Map<String, String> map) {
        this.map = map;
    }


    //-------------------------------------------------------------
    // Methods - Canonical
    //-------------------------------------------------------------


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("SampleClass");
        sb.append("{foo='").append(foo).append('\'');
        sb.append(", baz=").append(baz);
        sb.append(", number=").append(number);
        sb.append(", numberObject=").append(numberObject);
        sb.append(", bits=").append(bits == null ? "null" : "");
        for (int i = 0; bits != null && i < bits.length; ++i) {
            sb.append(i == 0 ? "" : ", ").append(bits[i]);
        }
        sb.append(", map=").append(map);
        sb.append('}');
        return sb.toString();
    }
}
