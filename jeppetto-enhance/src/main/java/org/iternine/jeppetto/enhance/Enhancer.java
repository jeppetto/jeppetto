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

package org.iternine.jeppetto.enhance;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class Enhancer<T> {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private final Class<T> baseClass;
    private Class<? extends T> enhancedClass;

    private static final Logger logger = LoggerFactory.getLogger(Enhancer.class);


    //-------------------------------------------------------------
    // Constructor
    //-------------------------------------------------------------

    protected Enhancer(Class<T> baseClass) {
        validateClassIsEnhanceable(baseClass);

        this.baseClass = baseClass;
    }


    //-------------------------------------------------------------
    // Methods - Abstract - Public
    //-------------------------------------------------------------

    public abstract boolean needsEnhancement(Object object);


    //-------------------------------------------------------------
    // Methods - Abstract - Protected
    //-------------------------------------------------------------

    protected abstract Class<? extends T> enhanceClass(Class<T> baseClass);


    //-------------------------------------------------------------
    // Methods - Public
    //-------------------------------------------------------------

    public void validateClassIsEnhanceable(Class<T> baseClass) {
        if (baseClass.isInterface()) {
            throw new IllegalArgumentException("Cannot enhance interface: " + baseClass.getSimpleName());
        }
    }


    public final Class<T> getBaseClass() {
        return baseClass;
    }


    public Class<? extends T> getEnhancedClass() {
        if (enhancedClass == null) {
            synchronized (baseClass) {
                if (enhancedClass == null) {
                    enhancedClass = enhanceClass(baseClass);
                }
            }
        }

        return enhancedClass;
    }


    /**
     * Creates a new object that is enhanced.
     *
     * @return new object
     */
    public T newInstance() {
        try {
            return getEnhancedClass().newInstance();
        } catch (Exception e) {
            logger.error("Could not instantiate enhanced object.", e);

            throw ExceptionUtil.propagate(e);
        }
    }


    /**
     * Enhances the given object.
     *
     * @param t object to enhance
     *
     * @return enhanced object
     */
    public T enhance(T t) {
        if (!needsEnhancement(t)) {
            return t;
        }

        try {
            return getEnhancedClass().getConstructor(baseClass).newInstance(t);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Could not enhance object %s (%s)", t, t.getClass()), e);
        }
    }
}
