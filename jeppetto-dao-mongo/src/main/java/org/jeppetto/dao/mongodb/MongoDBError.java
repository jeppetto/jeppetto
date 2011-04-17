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

package org.jeppetto.dao.mongodb;


/**
 * An error POJO.
 */
public class MongoDBError {

    //-------------------------------------------------------------
    // Constants
    //-------------------------------------------------------------

    public static final int NO_CODE = Integer.MIN_VALUE;


    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private int code = NO_CODE;
    private String err;


    //-------------------------------------------------------------
    // Methods - Public
    //-------------------------------------------------------------
    
    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getErr() {
        return err;
    }

    public void setErr(String err) {
        this.err = err;
    }
}
