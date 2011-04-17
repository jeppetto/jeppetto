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

package org.jeppetto.dao.mongodb.enhance;


import org.jeppetto.enhance.VelocityEnhancer;


public class DBObjectEnhancer<T> extends VelocityEnhancer<T> {

    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public DBObjectEnhancer(Class<T> baseClass) {
        super(baseClass);
    }


    //-------------------------------------------------------------
    // Implementation - Enhancer
    //-------------------------------------------------------------

    @Override
    protected boolean needsEnhancement(Object object) {
        return object != null && !(object instanceof com.mongodb.DBObject);
    }


    //-------------------------------------------------------------
    // Implementation - VelocityEnhancer
    //-------------------------------------------------------------

    @Override
    protected String getTemplateLocation() {
        return "org/jeppetto/dao/mongodb/enhance/dbobject.vm";
    }
}
