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

package org.iternine.jeppetto.dao.hibernate;


import org.iternine.jeppetto.dao.AccessControlContext;


public class AccessControlContextOverride {

    //-------------------------------------------------------------
    // Variables - Private - Static
    //-------------------------------------------------------------

    private static ThreadLocal<AccessControlContext> override = new ThreadLocal<AccessControlContext>();


    //-------------------------------------------------------------
    // Methods - Public
    //-------------------------------------------------------------

    public static boolean exists() {
        return override.get() != null;
    }


    public static AccessControlContext get() {
        return override.get();
    }


    public static void set(AccessControlContext accessControlContext) {
        override.set(accessControlContext);
    }


    public static void clear() {
        override.set(null);
    }
}
