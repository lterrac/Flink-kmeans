package it.polimi.middleware;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.Collection;

public class SelectNearestCenter extends RichMapFunction<Point, Tuple2<Integer, Point>> {

    private Collection<Centroid> centroids;

    /** Reads the centroid values from a broadcast variable into a collection. */
    @Override
    public void open(Configuration parameters) throws Exception {
        this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
    }

    @Override
    public Tuple2<Integer, Point> map(Point p) throws Exception {
        double min = Double.MAX_VALUE;
        int id = -1;
        for(Centroid centroid : centroids) {
            double distance = centroid.euclideanDistance(p);
            if(distance < min) {
                min = distance;
                id = centroid.id;
            }
        }
        return new Tuple2<>(id, p);
    }
}