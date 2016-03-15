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


import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class EnhancerTest {

    public static void main(String[] args) throws Exception {
        for (int i = 0; i < 5; i++) {
            doMain();
            System.gc();
            //Thread.sleep(2500L);
            //System.gc();
        }

        System.out.println("OK");
        Thread.sleep(25000000L);
    }

    public static void doMain()
            throws IllegalAccessException, InstantiationException {
        int n = 1000000;
        Set<SampleClass> test = new HashSet<SampleClass>();
        long before = System.nanoTime();
        for (int i = 0; i < n; i++) {
            test.add(new SampleClass());
        }
        System.out.format("%,dms to create %,d with constructor.%n", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - before), test.size());
        test.clear();
        Enhancer<SampleClass> e = EnhancerHelper.makePersistentEnhancer(SampleClass.class);
        test.add(e.enhance(new SampleClass()));
        n -= 1;
        before = System.nanoTime();
        for (int i = 0; i < n; i++) {
            test.add(e.enhance(new SampleClass()));
        }
        System.out.format("%,dms to wrap %,d with enhancer.%n", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - before), test.size());
        test.clear();
        n += 1;
        Class<? extends SampleClass> eclass = e.enhance(new SampleClass()).getClass();
        before = System.nanoTime();
        for (int i = 0; i < n; i++) {
            test.add(eclass.newInstance());
        }
        System.out.format("%,dms to create new %,d with enhanced class constructor.%n", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - before), test.size());
        test.clear();
        before = System.nanoTime();
        for (int i = 0; i < n; i++) {
            test.add(e.newInstance());
        }
        System.out.format("%,dms to create new %,d with enhancer.%n", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - before), test.size());
        before = System.nanoTime();
        Persistent p = (Persistent) test.iterator().next();
        boolean b = false;
        for (int i = 0; i < n; i++) {
            b |= p.isDirty();
        }
        System.out.format("%,dms to call isDirty %,d times (result: %b).%n", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - before), test.size(), b);
    }

    private Enhancer<SampleClass> enhancer; // ok to create multiple per test, should support that
    private Enhancer<SampleClass2> enhancer2; // ok to create multiple per test, should support that
    private Enhancer<SampleClassThatImplementsSnapshot> noop;


    @Before
    public void setup() {
        enhancer = EnhancerHelper.makePersistentEnhancer(SampleClass.class);
        enhancer2 = EnhancerHelper.makePersistentEnhancer(SampleClass2.class);
        noop = EnhancerHelper.makePersistentEnhancer(SampleClassThatImplementsSnapshot.class);
    }


    @Test
    @org.junit.Ignore
    public void doOnePerfRunForFunInTests()
            throws InstantiationException, IllegalAccessException {
        doMain();
    }


    @Test(expected = IllegalArgumentException.class)
    public void cannotCreateEnhancerForInterface() {
        EnhancerHelper.makePersistentEnhancer(Comparable.class);
    }


    @Test
    public void noopPassesBackObject() {
        SampleClassThatImplementsSnapshot o = new SampleClassThatImplementsSnapshot();
        assertSame(o, noop.enhance(o));
        assertSame(o.getClass(), noop.getEnhancedClass());
    }


    @Test
    public void implementsGetsDirty() {
        SampleClass enhanced = enhancer.enhance(new SampleClass());
        assertNotNull(enhanced);
        assertTrue(enhanced instanceof Persistent);
    }

    @Test
    public void enhancedClassIsNotSame() {
        assertNotSame(SampleClass.class, enhancer.getEnhancedClass());
    }

    @Test
    public void newInstanceIsDirtyWorks() {
        SampleClass enhanced = enhancer.newInstance();
        assertDirty(enhanced, true);
        ((Snapshot) enhanced).snapshot();
        assertDirty(enhanced, false);
        enhanced.setFoo("foo");
        assertDirty(enhanced, true);
        ((Snapshot) enhanced).snapshot();
        assertDirty(enhanced, false);
        enhanced.setFoo(null);
        assertDirty(enhanced, true);
        enhanced.setFoo("bar");
        assertDirty(enhanced, true);
        enhanced.setFoo("foo");
        assertDirty(enhanced, false);
    }

    @Test
    public void dirtyDetectionWorksInsideArrays() {
        SampleClass enhanced = enhancer.newInstance();
        ((Snapshot) enhanced).snapshot();
        enhanced.setBits(new boolean[] { false, true });
        assertDirty(enhanced, true);
        enhanced.setBits(new SampleClass().getBits());
        assertDirty(enhanced, false);
    }

    @Test
    public void enhancedIsDirtyWorks() {
        SampleClass testObject = new SampleClass();
        testObject.setFoo("bar");
        SampleClass enhanced = enhancer.enhance(testObject);
        ((Snapshot) enhanced).snapshot();
        assertDirty(enhanced, false);
        assertEquals("bar", enhanced.getFoo());
        enhanced.setFoo(null);
        assertNull(enhanced.getFoo());
        assertDirty(enhanced, true);
        ((Snapshot) enhanced).snapshot();
        assertDirty(enhanced, false);
    }

    @Test
    public void enhancedAnonymousSubclassWorks() {
        SampleClass testObject = new SampleClass() {{ setFoo("bar"); }};
        SampleClass enhanced = enhancer.enhance(testObject);
        ((Snapshot) enhanced).snapshot();
        assertDirty(enhanced, false);
        assertEquals("bar", enhanced.getFoo());
        enhanced.setFoo(null);
        assertNull(enhanced.getFoo());
        assertDirty(enhanced, true);
        ((Snapshot) enhanced).snapshot();
        assertDirty(enhanced, false);
    }

    @Test
    public void enhancedSubclassIsDirtyWorks() {
        SampleClass2 testObject2 = new SampleClass2();
        testObject2.setFoo(toString());
        long bar = System.nanoTime();
        testObject2.setBar(bar);
        assertEquals(toString(), testObject2.getFoo());
        assertEquals(bar, testObject2.getBar());
        SampleClass2 enhanced = enhancer2.enhance(testObject2);
        ((Snapshot) enhanced).snapshot();
        assertNotSame(testObject2, enhanced);
        assertNotNull(enhanced);
        assertDirty(enhanced, false);
        assertEquals(toString(), enhanced.getFoo());
        assertEquals(bar, enhanced.getBar());
        enhanced.setFoo(null);
        assertNull(enhanced.getFoo());
        assertDirty(enhanced, true);
        enhanced.setFoo(toString());
        assertDirty(enhanced, false);
        enhanced.setBar(0L);
        assertEquals(0L, enhanced.getBar());
        assertDirty(enhanced, true);
        enhanced.setFoo("something else");
        ((Snapshot) enhanced).snapshot(); // was dirty on two counts, one from super, one from sub
        assertDirty(enhanced, false);
    }

    @Test
    public void detectsMutationOfOriginal() {
        SampleClass testObject = new SampleClass();
        testObject.setFoo("bar");
        SampleClass enhanced = enhancer.enhance(testObject);
        ((Snapshot) enhanced).snapshot();
        assertDirty(enhanced, false);
        assertEquals("bar", enhanced.getFoo());
        testObject.setFoo(null);
        assertNull(enhanced.getFoo());
        assertDirty(enhanced, true);
        ((Snapshot) enhanced).snapshot();
        assertDirty(enhanced, false);
        testObject.setNumberObject(1234L);
        assertDirty(enhanced, true);
        assertEquals(1234L, enhanced.getNumberObject().longValue());
        ((Snapshot) enhanced).snapshot();
        assertDirty(enhanced, false);
    }

    @Test
    public void enhanceClassThatDoesntNeedIt() {
        SampleClass sampleClass = new SampleClassThatImplementsSnapshot();
        SampleClass enhanced = enhancer.enhance(sampleClass);
        assertNotNull(enhanced);
        assertTrue(enhanced instanceof Persistent);
        assertTrue(enhanced instanceof Snapshot);
        assertSame(sampleClass, enhanced);
    }

    @Test
    public void ifYouEnhancedNullYouGetNull() {
        assertNull(enhancer.enhance(null));
    }

    @Test
    public void enhancerAppearsThreadsafe() throws InterruptedException {
        int iter = 5000;
        int threads = 10;

        ExecutorService exec = Executors.newFixedThreadPool(threads);

        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threads * iter);
        final List<SampleClass2> newObjects = Collections.synchronizedList(new ArrayList<SampleClass2>());
        final List<Enhancer<SampleClass2>> enhancers = new ArrayList<Enhancer<SampleClass2>>();

        enhancers.add(EnhancerHelper.makePersistentEnhancer(SampleClass2.class));
        enhancers.add(EnhancerHelper.makePersistentEnhancer(SampleClass2.class));
        enhancers.add(EnhancerHelper.makePersistentEnhancer(SampleClass2.class));
        enhancers.add(EnhancerHelper.makePersistentEnhancer(SampleClass2.class));
        enhancers.add(EnhancerHelper.makePersistentEnhancer(SampleClass2.class));
        enhancers.add(EnhancerHelper.makePersistentEnhancer(SampleClass2.class));
        enhancers.add(EnhancerHelper.makePersistentEnhancer(SampleClass2.class));
        enhancers.add(EnhancerHelper.makePersistentEnhancer(SampleClass2.class));

        for (int i = 0; i < threads * iter; i++) {
            exec.submit(new Runnable() {
                public void run() {
                    try {
                        Enhancer<SampleClass2> enhancer = enhancers.get(newObjects.size() % enhancers.size());
                        SampleClass2 test = enhancer.newInstance();
                        ((Snapshot) test).snapshot();
                        start.await();

                        long bar = System.nanoTime();
                        assertDirty(test, false);
                        test.setBar(bar);
                        assertEquals(bar, test.getBar());
                        assertDirty(test, true);
                        newObjects.add(test);
                    } catch (Exception e) {
                        e.printStackTrace();
                        // failure will be caught by not enough objects in collection at end
                    } finally {
                        done.countDown();
                    }
                }
            });
        }

        start.countDown();
        done.await();

        assertEquals("Bug in test", 0, exec.shutdownNow().size());
        assertEquals(threads * iter, newObjects.size());
    }

    private void assertDirty(Object obj, boolean isDirty) {
        assertTrue(obj instanceof Persistent);
        assertEquals(isDirty, ((Persistent) obj).isDirty());
    }
}
