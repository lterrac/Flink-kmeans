package it.polimi.middleware;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.logging.Logger;

public class CentroidPointConverter implements MapFunction<Centroid, Tuple3<Integer, Double, Double>> {

    @Override
    public Tuple3<Integer, Double, Double> map(Centroid centroid) throws Exception {
        Logger.getLogger(Logger.GLOBAL_LOGGER_NAME).info(centroid.toString());
        return new Tuple3<>(centroid.id, centroid.x, centroid.y);
    }
}