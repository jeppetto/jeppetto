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

package org.iternine.jeppetto.dao;


import java.util.HashSet;
import java.util.Set;

public class SimpleAccessControlContext
        implements AccessControlContext {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private String accessId;
    private Set<String> roles;


    //-------------------------------------------------------------
    // Implementation - AccessControlContext
    //-------------------------------------------------------------

    public String getAccessId() {
        return accessId;
    }


    public Set<String> getRoles() {
        return roles;
    }


    //-------------------------------------------------------------
    // Methods - Setter
    //-------------------------------------------------------------

    public void setAccessId(String accessId) {
        this.accessId = accessId;
    }


    public void setRoles(Set<String> roles) {
        this.roles = roles;
    }
    
    
    public void addRole(String role) {
        if (roles == null) {
            this.roles = new HashSet<String>();
        }

        roles.add(role);
    }


    //-------------------------------------------------------------
    // Methods - Canonical
    //-------------------------------------------------------------

    @Override
    public String toString() {
        return "AccessControlContext{ accessId = '" + accessId + "', roles = " + roles + " }";
    }
}
