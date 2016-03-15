/*
 * Copyright (c) 2011-2014 Jeppetto and Jonathan Thompson
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

package org.iternine.jeppetto.enhance;


public class NoOpEnhancer<T> extends Enhancer<T> {

    //-------------------------------------------------------------
    // Constructor
    //-------------------------------------------------------------

    public NoOpEnhancer(Class<T> baseClass) {
        super(baseClass);
    }


    //-------------------------------------------------------------
    // Implementation - Enhancer
    //-------------------------------------------------------------

    @Override
    public boolean needsEnhancement(Object object) {
        return false;
    }


    @Override
    protected Class<? extends T> enhanceClass(Class<T> baseClass) {
        return null;
    }


    //-------------------------------------------------------------
    // Overrides - Enhancer
    //-------------------------------------------------------------

    @Override
    public Class<? extends T> getEnhancedClass() {
        return getBaseClass();
    }


    @Override
    public T newInstance() {
        try {
            return getBaseClass().newInstance();
        } catch (Exception e) {
            throw ExceptionUtil.propagate(e);
        }
    }


    @Override
    public T enhance(T obj) {
        return obj;
    }
}
