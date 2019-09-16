package it.polimi.middleware;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class CentroidAccumulator implements ReduceFunction<Tuple3<Integer, Point, Long>> {

    /**
     * f0 = centroid ID
     * f1 = Point
     * f2 = count -> number of points associated to the centroid
     * 
     * Returns a Tuple3 with fo = centroid ID , f1 is a new Point created by merging the two points of 
     * val1 and val2, f2 is the sum of the counts of val1 and val2
     * 
     */
    @Override
    public Tuple3<Integer, Point, Long> reduce(Tuple3<Integer, Point, Long> val1, Tuple3<Integer, Point, Long> val2) {
        return new Tuple3<>(val1.f0, val1.f1.add(val2.f1), val1.f2 + val2.f2);
    }
}