package it.polimi.middleware;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;

public class CalcNewError implements MapFunction<Tuple2<Centroid, Centroid>, Tuple1<Double>> {

    @Override
    public Tuple1<Double> map(Tuple2<Centroid, Centroid> value) {
        return new Tuple1<>(value.f0.euclideanDistance(value.f1));
    }
}