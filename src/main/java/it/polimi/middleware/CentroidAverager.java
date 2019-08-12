package it.polimi.middleware;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import sun.rmi.runtime.Log;

public class CentroidAverager implements MapFunction<Tuple3<Integer, Point, Long>, Centroid> {

    @Override
    public Centroid map(Tuple3<Integer, Point, Long> value) {
        return new Centroid(value.f0, value.f1.div(value.f2));
    }
}