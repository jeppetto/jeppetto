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

package org.iternine.jeppetto.dao.test;


public class RelatedObject {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private String id;
    private String relatedStringValue;
    private int relatedIntValue;


    //-------------------------------------------------------------
    // Methods - Getter/Setter
    //-------------------------------------------------------------

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }


    public String getRelatedStringValue() {
        return relatedStringValue;
    }


    public void setRelatedStringValue(String relatedStringValue) {
        this.relatedStringValue = relatedStringValue;
    }


    public int getRelatedIntValue() {
        return relatedIntValue;
    }


    public void setRelatedIntValue(int relatedIntValue) {
        this.relatedIntValue = relatedIntValue;
    }


    //-------------------------------------------------------------
    // Methods - Public
    //-------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RelatedObject)) {
            return false;
        }

        RelatedObject that = (RelatedObject) o;

        return relatedIntValue == that.relatedIntValue && (relatedStringValue == null ? that.relatedStringValue == null
                                                                                      : relatedStringValue.equals(that.relatedStringValue));
    }


    @Override
    public int hashCode() {
        return 31 * (relatedStringValue != null ? relatedStringValue.hashCode() : 0) + relatedIntValue;
    }
}
