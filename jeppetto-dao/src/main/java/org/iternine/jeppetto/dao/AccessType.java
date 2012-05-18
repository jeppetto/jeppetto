/*
 * Copyright (c) 2012 Jeppetto and Jonathan Thompson
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


public enum AccessType {

    //-------------------------------------------------------------
    // Enumeration Values
    //-------------------------------------------------------------

    None("N",
         new AllowsRule() {
             @Override
             public boolean allows(AccessType accessType) {
                 return false;
             }
         }),
    Read("R",
         new AllowsRule() {
             @Override
             public boolean allows(AccessType accessType) {
                 return AccessType.Read.ordinal() >= accessType.ordinal();
             }
         }),
    ReadWrite("RW",
              new AllowsRule() {
                  @Override
                  public boolean allows(AccessType accessType) {
                      return true;
                  }
              });


    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private String shortName;
    private AllowsRule allowsRule;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    AccessType(String shortName, AllowsRule allowsRule) {
        this.shortName = shortName;
        this.allowsRule = allowsRule;
    }


    //-------------------------------------------------------------
    // Methods - Public - Static
    //-------------------------------------------------------------

    public static AccessType getAccessTypeFromShortName(String shortName) {
        if ("R".equals(shortName)) {
            return AccessType.Read;
        } else if ("RW".equals(shortName)) {
            return AccessType.ReadWrite;
        } else if ("N".equals(shortName)) {
            return AccessType.None;
        } else {
            throw new RuntimeException("Unknown shortName: " + shortName);
        }
    }


    //-------------------------------------------------------------
    // Methods - Public
    //-------------------------------------------------------------

    public String shortName() {
        return shortName;
    }


    public boolean allows(AccessType accessType) {
        return allowsRule.allows(accessType);
    }


    //-------------------------------------------------------------
    // Inner Interface
    //-------------------------------------------------------------

    private interface AllowsRule {
        boolean allows(AccessType accessType);
    }
}
