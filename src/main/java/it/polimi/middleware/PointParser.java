package it.polimi.middleware;

public class PointParser {

    /**
     * This method parse text to a point.
     * @param text the point encoded in a string format like "0.12345 0.98765".
     * @return the point corresponding to the input text.
     */
    public static Point parse(String text) {
        String[] coordinates = text.split(" ");
        return new Point(
                Double.parseDouble(coordinates[0]),
                Double.parseDouble(coordinates[1])
        );
    }

}
