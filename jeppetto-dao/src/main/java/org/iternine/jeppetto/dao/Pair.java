package org.iternine.jeppetto.dao;


/**
 */
public class Pair<T1, T2> {

    //-------------------------------------------------------------
    // Variables - Private
    //-------------------------------------------------------------

    private T1 first;
    private T2 second;


    //-------------------------------------------------------------
    // Constructors
    //-------------------------------------------------------------

    public Pair() {
    }


    public Pair(T1 first, T2 second) {
        this.first = first;
        this.second = second;
    }


    //-------------------------------------------------------------
    // Methods - Getters
    //-------------------------------------------------------------

    public T1 getFirst() {
        return first;
    }


    public void setFirst(T1 first) {
        this.first = first;
    }


    public T2 getSecond() {
        return second;
    }


    public void setSecond(T2 second) {
        this.second = second;
    }


    //-------------------------------------------------------------
    // Methods - Object
    //-------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Pair pair = (Pair) o;

        return !(first != null ? !first.equals(pair.first) : pair.first != null)
               && !(second != null ? !second.equals(pair.second) : pair.second != null);

    }


    @Override
    public int hashCode() {
        int result = first != null ? first.hashCode() : 0;

        result = 31 * result + (second != null ? second.hashCode() : 0);

        return result;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Pair{");

        sb.append("Pair");
        sb.append("{ first=").append(first);
        sb.append(", second=").append(second);
        sb.append(" }");

        return sb.toString();
    }
}
