/*
 * Copyright (c) 2011-2017 Jeppetto and Jonathan Thompson
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

package org.iternine.jeppetto.dao.id;


import org.iternine.jeppetto.dao.Pair;


/**
 */
public class PairIdGenerator<T1, T2> implements IdGenerator<Pair<T1, T2>> {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private IdGenerator<T1> firstIdGenerator;
    private IdGenerator<T2> secondIdGenerator;


    //-------------------------------------------------------------
    // Implementation - IdGenerator
    //-------------------------------------------------------------

    @Override
    public Pair<T1, T2> generateId() {
        return new Pair<T1, T2>(firstIdGenerator.generateId(), secondIdGenerator.generateId());
    }


    //-------------------------------------------------------------
    // Methods - Getter/Setter
    //-------------------------------------------------------------

    public IdGenerator<T1> getFirstIdGenerator() {
        return firstIdGenerator;
    }


    public void setFirstIdGenerator(IdGenerator<T1> firstIdGenerator) {
        this.firstIdGenerator = firstIdGenerator;
    }


    public IdGenerator<T2> getSecondIdGenerator() {
        return secondIdGenerator;
    }


    public void setSecondIdGenerator(IdGenerator<T2> secondIdGenerator) {
        this.secondIdGenerator = secondIdGenerator;
    }
}
