package it.polimi.middleware;

public class CentroidParser {

    /**
     * This method parse text to a centroid.
     * @param text the centroid encoded in a string format like "1 0.12345 0.98765".
     * @return the centroid corresponding to the input text.
     */
    public static Centroid parse(String text) {
        String[] coordinates = text.split(" ");
        return new Centroid(
                Integer.parseInt(coordinates[0]),
                Double.parseDouble(coordinates[1]),
                Double.parseDouble(coordinates[2])
        );
    }

    /**
     * This method parse text to a centroid.
     * @param id the centroid id.
     * @param text the centroid encoded in a string format like "0.12345 0.98765".
     * @return the centroid corresponding to the input text.
     */
    public static Centroid parse(Integer id, String text) {
        String[] coordinates = text.split(" ");
        return new Centroid(
                id,
                Double.parseDouble(coordinates[0]),
                Double.parseDouble(coordinates[1])
        );
    }

}
