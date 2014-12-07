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

package org.iternine.jeppetto.dao.dynamodb.expression;


import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;

import org.iternine.jeppetto.dao.annotation.Transient;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class ProjectionExpressionBuilder extends ExpressionBuilder {

    //-------------------------------------------------------------
    // Constants
    //-------------------------------------------------------------

    private static final Set<Class<?>> DIRECTLY_PROJECTED_TYPES = new HashSet<Class<?>>() {{
        add(Date.class);
        add(boolean.class);
        add(char.class);
        add(byte.class);
        add(short.class);
        add(int.class);
        add(long.class);
        add(float.class);
        add(double.class);
        add(Number.class);
        add(BigDecimal.class);
        add(BigInteger.class);
        add(String.class);
        add(Boolean.class);
        add(Character.class);
        add(Byte.class);
        add(Short.class);
        add(Integer.class);
        add(Long.class);
        add(Float.class);
        add(Double.class);
    }};


    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private final Set<String> nonKeyAttributes;
    private final String expression;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public ProjectionExpressionBuilder(Class<?> entityClass, String hashKeyField, String rangeKeyField, String optimisticLockField) {
        super(false);

        this.nonKeyAttributes = new HashSet<String>();

        collectFields(entityClass, "", nonKeyAttributes);

        nonKeyAttributes.remove(hashKeyField);
        nonKeyAttributes.remove(rangeKeyField);

        if (optimisticLockField != null) {
            // Add the optimistic lock field explicitly since it may not be exposed in the entity directly.
            nonKeyAttributes.add(optimisticLockField);
        }

        StringBuilder expressionStringBuilder = new StringBuilder(hashKeyField);

        if (rangeKeyField != null) {
            expressionStringBuilder.append(", ").append(rangeKeyField);
        }

        for (String nonKeyAttribute : nonKeyAttributes) {
            expressionStringBuilder.append(", ").append(nonKeyAttribute);
        }

        this.expression = expressionStringBuilder.toString();
    }


    //-------------------------------------------------------------
    // Implementation - ExpressionBuilder
    //-------------------------------------------------------------

    @Override
    public boolean hasExpression() {
        return expression.length() > 0;
    }


    @Override
    public String getExpression() {
        // NB: nonKeyAttributes no longer used once the expression is fetched.  Could make variable non-final and set to null here.

        return expression;
    }


    @Override
    public String getExpressionAttributePrefix() {
        throw new RuntimeException("Should not be called");
    }


    //-------------------------------------------------------------
    // Methods - Public
    //-------------------------------------------------------------

    public Boolean isCoveredBy(Projection projection) {
        switch (ProjectionType.valueOf(projection.getProjectionType())) {
        case ALL:
            return Boolean.TRUE;
        case KEYS_ONLY:
            return nonKeyAttributes.isEmpty();
        case INCLUDE:
            return nonKeyAttributes.containsAll(projection.getNonKeyAttributes());
        }

        return Boolean.FALSE;
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private void collectFields(Class clazz, String fieldPrefix, Set<String> fields) {
        Map<String, Class> fieldsAndClasses = getFieldsAndClasses(clazz, fieldPrefix);

        for (Map.Entry<String, Class> entry : fieldsAndClasses.entrySet()) {
            String field = entry.getKey();
            Class fieldClass = entry.getValue();

            if (DIRECTLY_PROJECTED_TYPES.contains(fieldClass)
                || Collection.class.isAssignableFrom(fieldClass)) {
                fields.add(field);
            } else {
                collectFields(fieldClass, field + ".", fields);
            }
        }
    }


    private Map<String, Class> getFieldsAndClasses(Class clazz, String fieldPrefix) {
        Map<String, Class> fieldMap = new HashMap<String, Class>();

        List<Method> methods = new ArrayList<Method>();
        Collections.addAll(methods, clazz.getDeclaredMethods());
        Collections.addAll(methods, clazz.getMethods());

        for (Method method : methods) {
            if (method.getDeclaringClass().equals(Object.class)
                || method.getReturnType().equals(void.class)
                || Modifier.isFinal(method.getModifiers())
                || Modifier.isAbstract(method.getModifiers())
                || method.getParameterTypes().length != 0
                || method.getAnnotation(Transient.class) != null) {
                continue;
            }

            String methodName = method.getName();
            String upperCaseFieldName;

            if (methodName.startsWith("get")) {
                upperCaseFieldName = methodName.substring(3);
            } else if (methodName.startsWith("is")) {
                upperCaseFieldName = methodName.substring(2);
            } else {
                continue;
            }

            try {
                //noinspection unchecked
                clazz.getMethod("set".concat(upperCaseFieldName), method.getReturnType());
            } catch (NoSuchMethodException e) {
                continue;
            }

            String field = upperCaseFieldName.substring(0, 1).toLowerCase().concat(upperCaseFieldName.substring(1));

            fieldMap.put(fieldPrefix + field, method.getReturnType());
        }

        return fieldMap;
    }
}
