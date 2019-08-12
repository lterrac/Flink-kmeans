package it.polimi.middleware;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public final class TuplePointConverter implements MapFunction<Tuple2<Double, Double>, Point> {

    @Override
    public Point map(Tuple2<Double, Double> t) throws Exception {
        return new Point(t.f0, t.f1);
    }
}