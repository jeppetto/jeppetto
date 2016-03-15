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

package org.iternine.jeppetto.dao.updateobject;


import org.iternine.jeppetto.dao.annotation.Transient;
import org.iternine.jeppetto.enhance.VelocityEnhancer;

import javassist.CtMethod;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 */
public abstract class UpdateObjectVelocityEnhancer<T> extends VelocityEnhancer<T> {

    //-------------------------------------------------------------
    // Constants
    //-------------------------------------------------------------

    private static final Set<Class> NUMBER_CLASSES = new HashSet<Class>() {{
        add(byte.class);
        add(double.class);
        add(float.class);
        add(int.class);
        add(long.class);
        add(short.class);
        add(Number.class);
        add(BigDecimal.class);
        add(BigInteger.class);
        add(Byte.class);
        add(Double.class);
        add(Float.class);
        add(Integer.class);
        add(Long.class);
        add(Short.class);
    }};


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    protected UpdateObjectVelocityEnhancer(Class<T> baseClass) {
        super(baseClass);
    }


    //-------------------------------------------------------------
    // Implementation - VelocityEnhancer
    //-------------------------------------------------------------

    protected boolean shouldEnhanceMethod(CtMethod method) {
        return !method.hasAnnotation(Transient.class);
    }


    @Override
    protected Map<String, Object> getAdditionalContextItems() {
        Map<String, Object> contextItems = super.getAdditionalContextItems();
        List<Method> adders = new ArrayList<Method>();

        for (Method method : getBaseClass().getDeclaredMethods()) {
            String methodName = method.getName();

            // Validate the method is a valid, abstract adder
            if (methodName.startsWith("addTo")
                && Modifier.isAbstract(method.getModifiers())
                && method.getParameterTypes().length == 1
                && NUMBER_CLASSES.contains(method.getParameterTypes()[0])) {
                adders.add(method);
            }
        }

        contextItems.put("adders", adders.toArray(new Method[adders.size()]));

        return contextItems;
    }
}
