package it.polimi.middleware;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.Collection;

public class SelectPreviousCentroid extends RichMapFunction<Centroid, Tuple2<Centroid, Centroid>> {

    private Collection<Centroid> centroids;

    /** Reads the centroid values from a broadcast variable into a collection. */
    @Override
    public void open(Configuration parameters) throws Exception {
        this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
    }

    @Override
    public Tuple2<Centroid, Centroid> map(Centroid p) throws Exception {
        return new Tuple2<>(p, centroids.stream().filter(c -> c.id == p.id).findAny().get());
    }
}