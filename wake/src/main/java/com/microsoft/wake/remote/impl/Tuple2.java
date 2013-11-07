package com.microsoft.wake.remote.impl;

/**
 * Tuple with two values
 *
 * @param <T1>
 * @param <T2>
 */
public class Tuple2<T1, T2> {

    private final T1 t1;
    private final T2 t2;

    public Tuple2(T1 t1, T2 t2) {
        this.t1 = t1;
        this.t2 = t2;
    }

    public T1 getT1() {
        return t1;
    }

    public T2 getT2() {
        return t2;
    }

    @Override
    public int hashCode() {
        return t1.hashCode() + 31 * t2.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        Tuple2<T1, T2> tuple = (Tuple2<T1, T2>) o;
        return t1.equals((Object) tuple.getT1()) && t2.equals((Object) tuple.getT2());
    }

    public String toString() {
        return t1.toString() + " " + t2.toString();
    }
}