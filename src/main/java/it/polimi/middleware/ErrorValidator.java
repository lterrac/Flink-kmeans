package it.polimi.middleware;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;


public class ErrorValidator extends RichFilterFunction<Tuple1<Double>> implements FilterFunction<org.apache.flink.api.java.tuple.Tuple1<Double>> {

    private double threshold;

    /** Reads the centroid values from a broadcast variable into a collection. */
    @Override
    public void open(Configuration parameters) throws Exception {
        this.threshold = parameters.getDouble("error", 1);
    }

    @Override
    public boolean filter(Tuple1<Double> tuple) throws Exception {
        return tuple.f0 >= threshold;
    }

}
