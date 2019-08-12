package it.polimi.middleware;

import java.io.Serializable;

public class Point implements Serializable {

    public double x;
    public double y;

    public Point() {

    }

    public Point(double x, double y) {
        this.x = x;
        this.y = y;
    }

    public Point add(Point other) {
        return new Point(x + other.x, y + other.y);
    }

    public Point div(long val) {
        return new Point(x / val, y / val);
    }

    public double euclideanDistance(Point other) {
        return Math.sqrt((x - other.x) * (x - other.x) + (y - other.y) * (y - other.y));
    }

    public void clear() {
        x = 0.0;
        y = 0.0;
    }

    @Override
    public String toString() {
        return x + " " + y;
    }

}

