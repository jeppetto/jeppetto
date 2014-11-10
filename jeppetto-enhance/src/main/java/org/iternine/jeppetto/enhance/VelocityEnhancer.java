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


import javassist.ClassClassPath;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.NotFoundException;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public abstract class VelocityEnhancer<T> extends Enhancer<T> {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private Map<String, Object> contextItems;


    //-------------------------------------------------------------
    // Variables - Private - Static
    //-------------------------------------------------------------

    private static VelocityEngine engine;
    private static ClassPool pool;
    private static Logger logger = LoggerFactory.getLogger(VelocityEnhancer.class);


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    static {
        engine = new VelocityEngine();
        try {
            engine.setProperty(RuntimeConstants.RESOURCE_LOADER, "class");
            engine.setProperty("class.resource.loader.description", "Classpath Loader");
            engine.setProperty("class.resource.loader.class", ClasspathResourceLoader.class.getName());
            engine.setProperty(RuntimeConstants.RUNTIME_LOG_LOGSYSTEM_CLASS, "org.iternine.jeppetto.enhance.SLF4JLogChute");
            engine.setProperty("velocimacro.library", "");
            engine.setProperty("runtime.log.invalid.references", "false");

            engine.init();
        } catch (Exception e) {
            logger.error("Unrecoverable error initializing Velocity.", e);
        }

        pool = ClassPool.getDefault();
        pool.insertClassPath(new ClassClassPath(VelocityEnhancer.class));
    }


    public VelocityEnhancer(Class<T> baseClass) {
        super(baseClass);
    }


    public VelocityEnhancer(Class<T> baseClass, Map<String, Object> contextItems) {
        super(baseClass);

        this.contextItems = contextItems;
    }


    //-------------------------------------------------------------
    // Methods - Abstract - Protected
    //-------------------------------------------------------------

    protected abstract String getTemplateLocation();


    protected abstract boolean shouldEnhanceMethod(CtMethod method);


    //-------------------------------------------------------------
    // Implementation - Enhancer
    //-------------------------------------------------------------

    @Override
    public final Class<? extends T> enhanceClass(Class<T> baseClass) {
        logger.info("Enhancing {}", baseClass);

        CtClass original = null;

        try {
            original = pool.get(baseClass.getName());

            TemplateHelper templateHelper = new TemplateHelper(pool);
            VelocityContext velocityContext = new VelocityContext();

            velocityContext.put("_", templateHelper);
            velocityContext.put("base", original);
            velocityContext.put("getters", findGetters(original));
            velocityContext.put("abstractMethods", findAbstractMethods(original));

            if (contextItems != null) {
                for (Map.Entry<String, Object> contextItem : contextItems.entrySet()) {
                    velocityContext.put(contextItem.getKey(), contextItem.getValue());
                }
            }

            StringWriter writer = new StringWriter();
            engine.getTemplate(getTemplateLocation()).merge(velocityContext, writer);

            logger.debug("Enhanced {} to form new class {} with source:\n{}",
                         baseClass.getSimpleName(), templateHelper.clsName(), writer);

            return ClassLoadingUtil.toClass(templateHelper.compile());
        } catch (Exception e) {
            logger.error("An error occurred while enhancing {}", baseClass);

            throw ExceptionUtil.propagate(e);
        } finally {
            if (original != null) {
                original.detach();
            }
        }
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private Iterable<CtMethod> getMethodsFrom(CtClass superClass) {
        // return declared methods, then all methods (caller will de-dupe)
        List<CtMethod> methods = new ArrayList<CtMethod>();

        methods.addAll(Arrays.asList(superClass.getDeclaredMethods()));
        methods.addAll(Arrays.asList(superClass.getMethods()));

        return methods;
    }


    private CtMethod[] findGetters(CtClass superClass)
            throws NotFoundException, ClassNotFoundException {
        List<CtMethod> getters = new ArrayList<CtMethod>();
        String objectFullName = Object.class.getName();
        Set<String> handledMethods = new HashSet<String>();

        for (CtMethod method : getMethodsFrom(superClass)) {
            String methodName = method.getName();
            boolean methodIsGetter = methodName.startsWith("get") || methodName.startsWith("is");

            // Validate the method is a valid, overridable getter
            if (!handledMethods.contains(methodName)
                && methodIsGetter
                && !method.getDeclaringClass().getName().equals(objectFullName)
                && !Modifier.isFinal(method.getModifiers())
                && !Modifier.isAbstract(method.getModifiers())
                && method.getParameterTypes().length == 0
                && setterExists(superClass, extractFieldName(methodName))
                && shouldEnhanceMethod(method)) {

                handledMethods.add(methodName);
                getters.add(method);
            }
        }

        return getters.toArray(new CtMethod[getters.size()]);
    }


    private CtMethod[] findAbstractMethods(CtClass superClass) {
        List<CtMethod> abstractMethods = new ArrayList<CtMethod>();

        for (CtMethod method : getMethodsFrom(superClass)) {
            if (Modifier.isAbstract(method.getModifiers())) {
                abstractMethods.add(method);
            }
        }

        return abstractMethods.toArray(new CtMethod[abstractMethods.size()]);
    }


    private String extractFieldName(String from) {
        final String result;

        if (from.startsWith("is")) {
            result = from.substring(2);
        } else {
            result = from.substring(3);
        }

        if (result.length() > 1) {
            return result.substring(0, 1).toLowerCase().concat(result.substring(1));
        } else {
            return result.toLowerCase();
        }
    }


    private boolean setterExists(CtClass cls, String fieldName) {
        final String name;

        if (fieldName.length() > 1) {
            name = "set".concat(fieldName.substring(0, 1).toUpperCase()).concat(fieldName.substring(1));
        } else {
            name = "set".concat(fieldName.toUpperCase());
        }

        for (CtMethod method : cls.getMethods()) {
            if (name.equals(method.getName())) {
                return true;
            }
        }

        return false;
    }
}
