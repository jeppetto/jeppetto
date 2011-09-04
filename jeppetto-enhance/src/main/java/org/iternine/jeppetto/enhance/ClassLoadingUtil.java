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


import javassist.CannotCompileException;
import javassist.CtClass;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.ProtectionDomain;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Provides utility methods for defining javassist {@code CtClass} objects in
 * arbitrary {@code ClassLoader}s.
 * <p/>
 * This class is needed to define classes that sub-class those loaded in the system
 * classloader. For example, given a class {@code Foo}, and the code:
 * <pre>
 * CtClass myFooExtension = ...; // use javassist to 'extend' Foo and inject bytecode
 * Class cls = myFooExtension.toClass();
 * Foo foo = (Foo) cls.newInstance();
 * </pre>
 * The last line of this sample code would throw a ClassCastException because Foo, on the left,
 * was loaded by the system classloader and MyFooExtension, on the right, was loaded in
 * another classloader.
 * <p/>
 * To work around this, use this utility:
 * <pre>
 * CtClass myFooExtension = ...; // same as above
 * Class cls = ClassLoadingUtil.toClass(myFooExtension, Foo.class.getClassLoader(), null);
 * Foo foo = (Foo) cls.newInstance();
 * </pre>
 * </p>
 * And enjoy!
 */
public final class ClassLoadingUtil {

    //-------------------------------------------------------------
    // Utility Constructor
    //-------------------------------------------------------------

    private ClassLoadingUtil() { /* private utility constructor */ }


    //-------------------------------------------------------------
    // Constants
    //-------------------------------------------------------------

    private static final Lock LOCK = new ReentrantLock(false);
    private static final Method DEFINE_METHOD_NO_DOMAIN;
    private static final Method DEFINE_METHOD_WITH_DOMAIN;


    //-------------------------------------------------------------
    // Static Initializer
    //-------------------------------------------------------------

    static {
        try {
            LoggerFactory.getLogger(ClassLoadingUtil.class).info("Entering class initializer for ClassLoadingUtil. ClassLoader="
                                                                 + ClassLoadingUtil.class.getClassLoader());
        } catch (Exception e) {
            // bury
            e.printStackTrace();
        }

        try {
            Class<?> clsLoaderCls = Class.forName("java.lang.ClassLoader");
            DEFINE_METHOD_NO_DOMAIN = clsLoaderCls.getDeclaredMethod("defineClass",
                                                             String.class,
                                                             byte[].class,
                                                             int.class,
                                                             int.class);
            DEFINE_METHOD_WITH_DOMAIN = clsLoaderCls.getDeclaredMethod("defineClass",
                                                             String.class,
                                                             byte[].class,
                                                             int.class,
                                                             int.class,
                                                             ProtectionDomain.class);
        } catch (Exception e) {
            throw new RuntimeException("Could not initialize ClassLoadingUtil.");
        }
    }


    //-------------------------------------------------------------
    // Methods - Public - Static
    //-------------------------------------------------------------

    /**
     * Installs the given {@code CtClass} into the current class-loader and returns
     * it as a new class.
     *
     * @param ctClass class to load
     * @param <T> class type
     * @return new class
     * @throws CannotCompileException if the class cannot be compiled
     */
    public static <T> Class<T> toClass(CtClass ctClass) throws CannotCompileException {
        return toClass(ctClass, ClassLoadingUtil.class.getClassLoader(), null);
    }


    /**
     * Installs the given {@code CtClass} into the given class-loader and returns
     * it as a new class.
     *
     * @param ctClass class to load
     * @param loader class-loader to install into
     * @param domain protection domain, may be null
     * @param <T> class type
     * @return new class
     * @throws CannotCompileException if the class cannot be compiled
     */
    public static <T> Class<T> toClass(CtClass ctClass, ClassLoader loader, ProtectionDomain domain) throws CannotCompileException {
        try {
            byte[] byteCode = ctClass.toBytecode();

            if (domain == null) {
                return bruteForceDefineClass(DEFINE_METHOD_NO_DOMAIN, loader, ctClass.getName(), byteCode, 0, byteCode.length);
            } else {
                return bruteForceDefineClass(DEFINE_METHOD_WITH_DOMAIN, loader, ctClass.getName(), byteCode, 0, byteCode.length, domain);
            }
        } catch (Exception e) {
            ExceptionUtil.propagateIfInstanceOf(e, CannotCompileException.class);
            ExceptionUtil.propagateIfPossible(e);

            throw new CannotCompileException(e);
        } finally {
            ctClass.prune();
        }
    }


    //-------------------------------------------------------------
    // Methods - Private - Static
    //-------------------------------------------------------------

    @SuppressWarnings({"unchecked"})
    private static <T> Class<T> bruteForceDefineClass(Method method, ClassLoader loader, Object... args)
            throws InvocationTargetException, IllegalAccessException {

        LOCK.lock();
        try {
            assert !method.isAccessible() : "This method shouldn't be left accessible, something is wrong!";
            method.setAccessible(true);
            Class<T> cls = (Class<T>) method.invoke(loader, args);
            method.setAccessible(false);
            return cls;
        } finally {
            LOCK.unlock();
        }
    }
}
