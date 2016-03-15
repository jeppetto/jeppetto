/*
 * Copyright (c) 2011-2015 Jeppetto and Jonathan Thompson
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


package org.iternine.jeppetto.dao.dynamodb.extra.indexes;


public class Item {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private String hashKey;
    private String rangeKey;
    private String lsiField;
    private String lsiField2;
    private String gsiHashKey;
    private String gsiRangeKey;
    private String gsiRangeKey2;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public Item() {
    }


    // TODO: projections?


    //-------------------------------------------------------------
    // Methods - Builder
    //-------------------------------------------------------------

    public Item withKeyData(String hashKeyValue, String rangeKeyValue) {
        this.hashKey = hashKeyValue;
        this.rangeKey = rangeKeyValue;

        return this;
    }


    public Item withLSIData(String lsiFieldValue, String lsiField2Value) {
        this.lsiField = lsiFieldValue;
        this.lsiField2 = lsiField2Value;

        return this;
    }


    public Item withGSIData(String gsiHashKeyValue, String gsiRangeKeyValue, String gsiRangeKey2Value) {
        this.gsiHashKey = gsiHashKeyValue;
        this.gsiRangeKey = gsiRangeKeyValue;
        this.gsiRangeKey2 = gsiRangeKey2Value;
        
        return this;
    }


    //-------------------------------------------------------------
    // Methods - Getter/Setter
    //-------------------------------------------------------------

    public String getHashKey() {
        return hashKey;
    }


    public void setHashKey(String hashKey) {
        this.hashKey = hashKey;
    }


    public String getRangeKey() {
        return rangeKey;
    }


    public void setRangeKey(String rangeKey) {
        this.rangeKey = rangeKey;
    }


    public String getLsiField() {
        return lsiField;
    }


    public void setLsiField(String lsiField) {
        this.lsiField = lsiField;
    }


    public String getLsiField2() {
        return lsiField2;
    }


    public void setLsiField2(String lsiField2) {
        this.lsiField2 = lsiField2;
    }


    public String getGsiHashKey() {
        return gsiHashKey;
    }


    public void setGsiHashKey(String gsiHashKey) {
        this.gsiHashKey = gsiHashKey;
    }


    public String getGsiRangeKey() {
        return gsiRangeKey;
    }


    public void setGsiRangeKey(String gsiRangeKey) {
        this.gsiRangeKey = gsiRangeKey;
    }


    public String getGsiRangeKey2() {
        return gsiRangeKey2;
    }


    public void setGsiRangeKey2(String gsiRangeKey2) {
        this.gsiRangeKey2 = gsiRangeKey2;
    }


    //-------------------------------------------------------------
    // Methods - Canonical
    //-------------------------------------------------------------


    @Override
    public String toString() {
        return "Item{" + "hashKey='" + hashKey + '\'' + ", rangeKey='" + rangeKey + '\'' + ", lsiField='" + lsiField + '\''
               + ", lsiField2='" + lsiField2 + '\'' + ", gsiHashKey='" + gsiHashKey + '\'' + ", gsiRangeKey='" + gsiRangeKey + '\''
               + ", gsiRangeKey2='" + gsiRangeKey2 + '\'' + '}';
    }
}
