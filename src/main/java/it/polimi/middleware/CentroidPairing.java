package it.polimi.middleware;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.Collection;

public class CentroidPairing extends RichMapFunction<Centroid, Tuple2<Centroid, Centroid>> {

    private Collection<Centroid> centroids;

    /** Reads the centroid values from a broadcast variable into a collection. */
    @Override
    public void open(Configuration parameters) throws Exception {
        this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
    }

    @Override
    public Tuple2<Centroid, Centroid> map(Centroid centroid) throws Exception {
        Centroid oldCentroid = centroids.stream()
                .filter(c -> c.id == centroid.id)
                .findAny()
                .get();
        return new Tuple2<>(oldCentroid, centroid);
    }

}