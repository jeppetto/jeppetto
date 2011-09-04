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
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.NotFoundException;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Method;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class ClassLoadingUtilTest {

    //-------------------------------------------------------------
    // Variables - Private - Static
    //-------------------------------------------------------------

    private static ClassPool pool;
    private static AtomicInteger count;


    //-------------------------------------------------------------
    // Methods - Public - Static
    //-------------------------------------------------------------

    @BeforeClass
    public static void setupTest() {
        pool = ClassPool.getDefault();
        count = new AtomicInteger(0);
    }


    //-------------------------------------------------------------
    // Tests
    //-------------------------------------------------------------

    @Test
    public void basicSubclassWorks()
            throws NotFoundException, CannotCompileException, IllegalAccessException, InstantiationException {
        CtClass superClass = pool.get(TestClassAlpha.class.getName());
        CtClass subClass = pool.makeClass(makeNewName());
        subClass.setSuperclass(superClass);
        CtMethod toStringMethod = superClass.getDeclaredMethod("toString");
        CtMethod override = CtNewMethod.copy(toStringMethod, "toString", subClass, null); 
        override.setBody("return ($r) super.toString().concat(\"Beta!\");");
        subClass.addMethod(override);
        Class<TestClassAlpha> betaClass = ClassLoadingUtil.toClass(subClass);
        TestClassAlpha beta = betaClass.newInstance();
        assertEquals("AlphaBeta!", beta.toString());
    }



    // This test attempts to verify that concurrent usage of the utility doesn't leave the ClassLoader.defineClass
    // method in a wonky state.
    @Test
    public void utilityAppearsThreadSafe()
            throws Exception {
        int threads = 10;
        int iterations = 500;
        ExecutorService exec = Executors.newFixedThreadPool(threads);

        final Method defineClass1 = ClassLoader.class.getDeclaredMethod("defineClass", String.class, byte[].class, int.class, int.class);
        final Method defineClass2 = ClassLoader.class.getDeclaredMethod("defineClass", String.class, byte[].class, int.class, int.class, ProtectionDomain.class);
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threads * iterations);
        final List<Class<?>> newClasses = Collections.synchronizedList(new ArrayList<Class<?>>(threads));

        for (int i = 0; i < threads * iterations; i++) {
            exec.submit(new Runnable() {
                public void run() {
                    try {
                        String newName = makeNewName();
                        CtClass newClass = pool.makeClass(newName);

                        start.await();

                        // aggressively test isAccessible on the methods. they should only be accessible to
                        // one thread at a time, only within the toClass method, for a very brief time
                        assertFalse(defineClass1.isAccessible());
                        assertFalse(defineClass2.isAccessible());
                        Class<Object> theNewClass = ClassLoadingUtil.toClass(newClass);
                        assertFalse(defineClass1.isAccessible());
                        assertFalse(defineClass2.isAccessible());
                        newClasses.add(theNewClass);
                    } catch (Exception e) {
                        // ignore
                    } finally {
                        done.countDown();
                    }
                }
            });
        }

        start.countDown();
        done.await();

        assertEquals("Bug in test", 0, exec.shutdownNow().size());
        assertEquals(threads * iterations, newClasses.size());
        assertFalse(defineClass1.isAccessible());
        assertFalse(defineClass2.isAccessible());
    }


    @Test
    public void enhanceGamma()
            throws Exception {
        CtClass gamma = pool.get(TestClassGamma.class.getName());
        CtClass dirtyInterface = pool.get(Persistent.class.getName());

        CtClass enhanced = pool.makeClass(makeNewName());
        enhanced.setSuperclass(gamma);
        enhanced.addInterface(dirtyInterface);

        CtMethod isDirtyMethod = CtNewMethod.make("public boolean isDirty() { return getFoo() != null || getBars() != null; }", enhanced);
        enhanced.addMethod(isDirtyMethod);

        Class<TestClassGamma> cls = ClassLoadingUtil.toClass(enhanced);
        TestClassGamma instance = cls.newInstance();

        assertNotNull(instance);
        assertNull(instance.getBars());
        assertNull(instance.getFoo());
        assertDirty(instance, false);
        instance.setFoo(toString());
        instance.setBars(Collections.singletonList("bar"));
        assertDirty(instance, true);
        assertEquals(toString(), instance.getFoo());
        assertEquals(Collections.singletonList("bar"), instance.getBars());
        instance.setFoo(null);
        instance.setBars(null);
        assertNull(instance.getBars());
        assertNull(instance.getFoo());
        assertDirty(instance, false);
    }


    //-------------------------------------------------------------
    // Methods - Private
    //-------------------------------------------------------------

    private static String makeNewName() {
        return String.format("%s$$Sub%d", ClassLoadingUtilTest.class.getName(), count.incrementAndGet());
    }


    private void assertDirty(Object obj, boolean isDirty) {
        assertTrue(obj instanceof Persistent);
        assertEquals(isDirty, ((Persistent) obj).isDirty());
    }


    //-------------------------------------------------------------
    // Inner Class - TestClassAlpha
    //-------------------------------------------------------------

    public static class TestClassAlpha {

        //-------------------------------------------------------------
        // Methods - Canonical
        //-------------------------------------------------------------

        @Override
        public String toString() {
            return "Alpha";
        }
    }


    //-------------------------------------------------------------
    // Inner Class - TestClassGamma
    //-------------------------------------------------------------

    public static class TestClassGamma {

        //-------------------------------------------------------------
        // Variables - Private
        //-------------------------------------------------------------

        private String foo;
        private List<String> bars;


        //-------------------------------------------------------------
        // Methods - Getter/Setter
        //-------------------------------------------------------------

        public String getFoo() {
            return foo;
        }


        public void setFoo(String foo) {
            this.foo = foo;
        }


        public List<String> getBars() {
            return bars;
        }


        public void setBars(List<String> bars) {
            this.bars = (bars == null) ? null : new ArrayList<String>(bars);
        }
    }
}
