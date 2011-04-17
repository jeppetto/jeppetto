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

package org.jeppetto.enhance;


import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtConstructor;
import javassist.CtField;
import javassist.CtMethod;
import javassist.CtNewConstructor;
import javassist.CtNewMethod;
import javassist.NotFoundException;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


public class TemplateHelper {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private static AtomicInteger COUNT = new AtomicInteger(1);  // A counter to uniquify enhanced class names, starts at 1.
    private static Map<Class<?>, Class<?>> PRIMITIVE_WRAPPERS;

    private String className;
    private String superClassName;
    private List<String> interfaceNames = new ArrayList<String>();
    private CtClass thisClass;
    private ClassPool pool;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    static {
        PRIMITIVE_WRAPPERS = new HashMap<Class<?>, Class<?>>();

        PRIMITIVE_WRAPPERS.put(boolean.class, Boolean.class);
        PRIMITIVE_WRAPPERS.put(byte.class, Byte.class);
        PRIMITIVE_WRAPPERS.put(short.class, Short.class);
        PRIMITIVE_WRAPPERS.put(int.class, Integer.class);
        PRIMITIVE_WRAPPERS.put(long.class, Long.class);
        PRIMITIVE_WRAPPERS.put(float.class, Float.class);
        PRIMITIVE_WRAPPERS.put(double.class, Double.class);
        PRIMITIVE_WRAPPERS.put(void.class, Void.class);
    }


    public TemplateHelper(ClassPool pool) {
        this.pool = pool;
    }


    //-------------------------------------------------------------
    // Methods - Public
    //-------------------------------------------------------------

    public TemplateHelper cls(String name) {
        String newClassName = new StringBuilder(name).append("$").append(COUNT.getAndIncrement()).toString();

        thisClass = pool.makeClass(newClassName);
        className = thisClass.getSimpleName();

        return this;
    }


    public String clsName() {
        return className;
    }


    public TemplateHelper ext(CtClass superClass)
            throws CannotCompileException {
        superClassName = superClass.getName();
        thisClass.setSuperclass(superClass);

        return this;
    }


    public TemplateHelper impl(String interfaceName)
            throws NotFoundException {
        interfaceNames.add(interfaceName);
        thisClass.addInterface(pool.get(interfaceName));

        return this;
    }


    public String field(String code)
            throws CannotCompileException {
        CtField field = CtField.make(code, thisClass);
        thisClass.addField(field);

        return decorateForDebug(code);
    }


    public String ctor(String code)
            throws CannotCompileException {
        CtConstructor constructor = CtNewConstructor.make(code, thisClass);

        thisClass.addConstructor(constructor);

        return decorateForDebug(code);
    }


    public String method(String code)
            throws CannotCompileException {
        CtMethod m = CtNewMethod.make(code, thisClass);

        thisClass.addMethod(m);

        return decorateForDebug(code);
    }


    public String asSetter(CtMethod getter) {
        String getterName = getter.getName();
        String key = keyFor(getterName);

        return String.format("set%s%s", Character.toUpperCase(key.charAt(0)), key.substring(1));
    }


    public String keyFor(String getterName) {
        String sub;

        if (getterName.startsWith("is")) {
            sub = getterName.substring(2);
        } else if (getterName.startsWith("get")) {
            sub = getterName.substring(3);
        } else {
            throw new RuntimeException("Unexpected getter: " + getterName);
        }

        return sub.substring(0, 1).toLowerCase().concat(sub.substring(1));
    }


    public Class<?> returnTypeOf(CtMethod method)
            throws ClassNotFoundException, NoSuchMethodException {
        Method rawMethod = getRawMethod(method);

        return getGenericTypePair(rawMethod.getGenericReturnType()).getFirst();
    }


    public Class<?>[] returnTypeParamsOf(CtMethod method)
            throws ClassNotFoundException, NoSuchMethodException {
        Method rawMethod = getRawMethod(method);

        return getGenericTypePair(rawMethod.getGenericReturnType()).getSecond();
    }


    public String wrapperNameFor(Class<?> cls) {
        assert cls.isPrimitive() : "Don't call this with a non-primitive type.";

        for (Class<?> primitive : PRIMITIVE_WRAPPERS.keySet()) {
            if (primitive.getName().equals(cls.getName())) {
                return PRIMITIVE_WRAPPERS.get(primitive).getName();
            }
        }

        throw new RuntimeException("No primitive type found for " + cls);
    }


    public CtClass compile() {
        return thisClass;
    }


    public String createConversionMethodBody(Class<?> returnType) {
        if (returnType.isPrimitive() && PRIMITIVE_WRAPPERS.containsKey(returnType)) {
            if (Number.class.isAssignableFrom(PRIMITIVE_WRAPPERS.get(returnType))) {
                return "return ((Number) o)." + returnType.getSimpleName() + "Value();";
            }
        }

        return "throw new RuntimeException(\"Not sure how to convert \" + o + \" to a " + returnType.getSimpleName() + "\");";
    }


    //-------------------------------------------------------------
    // Override - Object
    //-------------------------------------------------------------

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("class ").append(className);

        if (superClassName != null) {
            sb.append(" extends ").append(superClassName);
        }

        if (!interfaceNames.isEmpty()) {
            sb.append(" implements ");

            for (int i = 0; i < interfaceNames.size(); i++) {
                if (i > 0) {
                    sb.append(", ");
                }

                sb.append(interfaceNames.get(i));
            }
        }

        return sb.toString();
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private String decorateForDebug(String code) {
        return code;
    }


    @SuppressWarnings({"ConstantConditions"})
    private Pair<Class<?>, Class<?>[]> getGenericTypePair(Type type) {
        if (ParameterizedType.class.isAssignableFrom(type.getClass())) {
            Class<?>[] optionalValueTypes = null;

            @SuppressWarnings({"ConstantConditions"})
            ParameterizedType ptype = (ParameterizedType) type;

            if (Map.class.isAssignableFrom((Class<?>) ptype.getRawType())) {
                optionalValueTypes = new Class<?>[] { (Class<?>) ptype.getActualTypeArguments()[0],
                                                      (Class<?>) ptype.getActualTypeArguments()[1] };
            } else if (Iterable.class.isAssignableFrom((Class<?>) ptype.getRawType())) {
                optionalValueTypes = new Class<?>[] { (Class<?>) ptype.getActualTypeArguments()[0] };
            }

            return new Pair<Class<?>, Class<?>[]>((Class<?>) ptype.getRawType(), optionalValueTypes);
        } else {
            return new Pair<Class<?>, Class<?>[]>((Class<?>) type, new Class<?>[]{});
        }
    }


    private Method getRawMethod(CtMethod method)
            throws ClassNotFoundException, NoSuchMethodException {
        Class<?> rawClass = Class.forName(method.getDeclaringClass().getName());

        return rawClass.getMethod(method.getName());
    }


    //-------------------------------------------------------------
    // Inner Classes
    //-------------------------------------------------------------

    public class Pair<E, F> {

        //-------------------------------------------------------------
        // Variables - Private
        //-------------------------------------------------------------

        private E first;
        private F second;


        //-------------------------------------------------------------
        // Constructors
        //-------------------------------------------------------------

        public Pair(E first, F second) {
            this.first = first;
            this.second = second;
        }


        //-------------------------------------------------------------
        // Methods - Getter/Setter
        //-------------------------------------------------------------

        public E getFirst() {
            return first;
        }


        public F getSecond() {
            return second;
        }
    }
}
