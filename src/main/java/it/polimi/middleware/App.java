package it.polimi.middleware;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Collection;
import java.util.LinkedList;

public class App {

    public static void main(String[] args) throws Exception {

        //Reading args
        String pointsPath = args[1];
        int numberOfPoints = Integer.parseInt(args[2]);
        String centroidsPath = args[3];
        int numberOfCentroids = Integer.parseInt(args[4]);
        int typeOfDistance = Integer.parseInt(args[5]);
        double error = Double.parseDouble(args[6]);
        String outputFile = args[7];
        int parallesism = Integer.parseInt(args[8]);


        Configuration configuration = new Configuration();
        configuration.setDouble("error", error);

        //Execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallesism);

        //Reading input points
        Collection<Point> pointsCollection = new LinkedList<>();
        BufferedReader reader = new BufferedReader(new FileReader(pointsPath));
        for(int i = 0; i < numberOfPoints; i++ ) {
            pointsCollection.add(PointParser.parse(reader.readLine()));
        }
        //Converting the input points to a dataset of points
        DataSet<Point> points = env.fromCollection(pointsCollection).setParallelism(1);

        //Reading input centroids
        Collection<Centroid> centroidsCollection = new LinkedList<>();
        reader = new BufferedReader(new FileReader(centroidsPath));
        for(int i = 0; i < numberOfCentroids; i++ ) {
            centroidsCollection.add(CentroidParser.parse(i, reader.readLine()));
        }
        //Converting the input centroid to a dataset of centroids
        DataSet<Centroid> centroids = env.fromCollection(centroidsCollection).setParallelism(1);

        //Declaring the iterative dataset
        //maxIterations is set to 20000, the end termination criterion is not based on the
        //max amount of iterations but on the error from one iteration and its next
        IterativeDataSet<Centroid> loop = centroids.iterate(20000);

        //Computation
        //Create a Dataset of Tuple2<centroid, current error>
        DataSet<Tuple2<Centroid, Double>> newCentroids = points
                //For each point find the closest centroid.
                //the broadcast set is needed in order to make the points 'aware' of the centroids.
                .map(new SelectNearestCenter()).withBroadcastSet(loop, "centroids")
                //Now we have a Tuple2<ID_centroid, point>
                //Add a long to the point with value 1L to count the number of points close to the given centroid
                .map(new CountAppender())
                //Now we have a Tuple3<ID_centroid, point,number of points associated to the centroid(initialized to 1L)>
                //Now the DataSet is equal to the number of points 
                //Group the points by the id of the closest centroid
                .groupBy(0)
                //Accumulate all points that are close to the same centroid
                .reduce(new CentroidAccumulator())
                //Now the DataSet is equal to the number of centroids
                //Compute the new centroid by averaging the results.
                .map(new CentroidAverager())
                //Now it is a Centroid DataSet
                .map(new SelectPreviousCentroid()).withBroadcastSet(loop, "centroids")
                //Tuple2<NewCentroid, OldCentroid> 
                .map(new CalcNewError());
                //Tuple2<Centroid, Error>

        //Termination criterion
        DataSet<Centroid> finalCentroids = loop.closeWith(
                //Returns only the centroids starting from Tuple2<Centroid, Error>
                newCentroids.
                        map(new RichMapFunction<Tuple2<Centroid, Double>, Centroid>() {
                            @Override
                            public Centroid map(Tuple2<Centroid, Double> t) throws Exception {
                                return t.f0;
                            }
                        }),
                //Termination criterion: sum all the errors and check if the sum is smaller than the threshold
                //                       and if it is stop the computation
                newCentroids
                        .map(new RichMapFunction<Tuple2<Centroid, Double>, Tuple1<Double>>() {
                            @Override
                            public Tuple1<Double> map(Tuple2<Centroid, Double> t) throws Exception {
                                return new Tuple1<>(t.f1);
                            }
                        })
                        .sum(0)
                        .filter(new ErrorValidator()).withParameters(configuration));

        //finalCentroids.print();

        //Write the result to the output file
        DataSet<Tuple3<Integer, Double, Double>> toTuple = finalCentroids.map(new CentroidPointConverter());
        toTuple.writeAsCsv(outputFile, "\n", " ");


        //Run the computation
        env.execute("KMEANS_POINTS_"+points+"_CENTROID_"+centroids+"_PARALLELISM_"+parallesism);
    }

}
