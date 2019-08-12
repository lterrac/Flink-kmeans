package it.polimi.middleware;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
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


        Configuration configuration = new Configuration();
        configuration.setDouble("error", error);

        //Execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        //Reading input points
        Collection<Point> pointsCollection = new LinkedList<>();
        BufferedReader reader = new BufferedReader(new FileReader(pointsPath));
        for(int i = 0; i < numberOfPoints; i++ ) {
            pointsCollection.add(PointParser.parse(reader.readLine()));
        }
        //Converting the input points to a dataset of points
        DataSet<Point> points = env.fromCollection(pointsCollection);

        //Reading input centroids
        Collection<Centroid> centroidsCollection = new LinkedList<>();
        reader = new BufferedReader(new FileReader(centroidsPath));
        for(int i = 0; i < numberOfCentroids; i++ ) {
            centroidsCollection.add(CentroidParser.parse(i, reader.readLine()));
        }
        //Converting the input centroid to a dataset of centroids
        DataSet<Centroid> centroids = env.fromCollection(centroidsCollection);

        //Declaring the iterative dataset
        //maxIterations is set to 1000, the end termination criterion is not based on the
        //max amount of iterations but on the error from one iteration and its next
        IterativeDataSet<Centroid> loop = centroids.iterate(100);

        //Computation
        DataSet<Centroid> newCentroids = points
                //For each point find the closest centroid.
                //the broadcast set is needed in order to make the points 'aware' of the centroids.
                .map(new SelectNearestCenter()).withBroadcastSet(loop, "centroids")
                //Add a long to the point with value 1L
                .map(new CountAppender())
                //Group the points by the id of the closest centroid
                .groupBy(0)
                //Accumulate all points that are close to the same centroid
                .reduce(new CentroidAccumulator())
                //Compute the new centroid by averaging the results.
                .map(new CentroidAverager());

        //Termination criterion
        DataSet<Centroid> finalCentroids = loop.closeWith(newCentroids);
        /*
        DataSet<Centroid> finalCentroids = loop.closeWith(newCentroids, newCentroids
                .join(centroids).where(c -> c.id).equalTo(c -> c.id)
                .map(new CalcNewError())
                .sum(0)
                .filter(new ErrorValidator()).withParameters(configuration));

        finalCentroids.print();
         */

        //Write the result to the output file
        DataSet<Tuple3<Integer, Double, Double>> toTuple = finalCentroids.map(new CentroidPointConverter());
        toTuple.writeAsCsv(outputFile, "\n", " ");


        //Run the computation
        env.execute("KMeans Example");

    }

}