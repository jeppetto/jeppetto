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

package org.iternine.jeppetto.dao.hibernate;


public class AccessControlEntry {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private long id;
    private String objectType;
    private String objectId;
    private String accessibleBy;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public AccessControlEntry() {
    }


    //-------------------------------------------------------------
    // Methods - Getter/Setter
    //-------------------------------------------------------------

    public long getId() {
        return id;
    }


    public void setId(long id) {
        this.id = id;
    }


    public String getObjectType() {
        return objectType;
    }


    public void setObjectType(String objectType) {
        this.objectType = objectType;
    }


    public String getObjectId() {
        return objectId;
    }


    public void setObjectId(String objectId) {
        this.objectId = objectId;
    }


    public String getAccessibleBy() {
        return accessibleBy;
    }


    public void setAccessibleBy(String accessibleBy) {
        this.accessibleBy = accessibleBy;
    }
}
