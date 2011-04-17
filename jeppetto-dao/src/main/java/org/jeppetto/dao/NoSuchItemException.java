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

package org.jeppetto.dao;


public class NoSuchItemException extends Exception {

    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public NoSuchItemException() {
    }


    public NoSuchItemException(String message) {
        super(message);
    }


    /**
     * Return an exception which says that no item of type itemType
     * with identifier 'identifier' was found.
     * @param itemType A string describing the type of item
     * @param identifier A string identifier for the item
     */
    public NoSuchItemException(String itemType, String identifier) {
        super("No item of type '" + itemType + "' identified by '" + identifier + "' was found.");
    }
}
