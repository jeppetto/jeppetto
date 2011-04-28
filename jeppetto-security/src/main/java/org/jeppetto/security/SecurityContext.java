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

package org.jeppetto.security;


public class SecurityContext {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private String securityContextKey;
    private String targetId;
    private String friendlyName;
    private String role;


    //-------------------------------------------------------------
    // Variables - Private - Static
    //-------------------------------------------------------------

    private static ThreadLocal<SecurityContext> threadLocalSecurityContext = new ThreadLocal<SecurityContext>();


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public SecurityContext() {
    }


    //-------------------------------------------------------------
    // Methods - Public - Static
    //-------------------------------------------------------------

    public static SecurityContext getCurrent() {
        return threadLocalSecurityContext.get();
    }


    public static void setCurrent(SecurityContext securityContext) {
        threadLocalSecurityContext.set(securityContext);
    }


    public static void clearCurrent() {
        threadLocalSecurityContext.remove();
    }


    //-------------------------------------------------------------
    // Methods - Getter/Setter
    //-------------------------------------------------------------

    public String getSecurityContextKey() {
        return securityContextKey;
    }


    public void setSecurityContextKey(String securityContextKey) {
        this.securityContextKey = securityContextKey;
    }


    public String getTargetId() {
        return targetId;
    }


    public void setTargetId(String targetId) {
        this.targetId = targetId;
    }


    public String getFriendlyName() {
        return friendlyName;
    }


    public void setFriendlyName(String friendlyName) {
        this.friendlyName = friendlyName;
    }


    public String getRole() {
        return role;
    }


    public void setRole(String role) {
        this.role = role;
    }


    //-------------------------------------------------------------
    // Methods - Canonical
    //-------------------------------------------------------------

    @Override
    public String toString() {
        return "SecurityContext { "
               + "securityContextKey = " + securityContextKey
               + ", targetId = " + targetId
               + ", friendlyName = " + friendlyName
               + ", role = " + role
               + '}';
    }
}
