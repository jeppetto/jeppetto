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

package org.jeppetto.testsupport;


import static org.junit.Assert.fail;

import java.math.BigDecimal;


public class AssertUtil {

    //-------------------------------------------------------------
    // Constructor
    //-------------------------------------------------------------

    private AssertUtil() {
    }


    //-------------------------------------------------------------
    // Methods - Public - Static
    //-------------------------------------------------------------

    public static void assertDecimalValueEquals(String msg, BigDecimal expectedDecimal, BigDecimal actualDecimal) {
        boolean areEqual = true;

        if (expectedDecimal == null) {
            if (actualDecimal != null) {
                areEqual = false;
            }
        } else {
            if (expectedDecimal.compareTo(actualDecimal) != 0) {
                areEqual = false;
            }
        }
        
        if (!areEqual) {
            fail(msg + ": expected (" + expectedDecimal + ") was actual (" + actualDecimal + ")");
        }
    }



}
