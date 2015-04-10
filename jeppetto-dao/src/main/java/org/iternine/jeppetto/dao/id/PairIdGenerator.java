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
