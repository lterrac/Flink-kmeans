package it.polimi.middleware;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public final class TupleCentroidConverter implements MapFunction<Tuple3<Integer, Double, Double>, Centroid> {

    @Override
    public Centroid map(Tuple3<Integer, Double, Double> t) throws Exception {
        return new Centroid(t.f0, t.f1, t.f2);
    }
}
