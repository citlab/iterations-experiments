package org.apache.flink.examples.java.ml.util;

import java.io.Serializable;

/**
 * A simple data sample, x means the input, and y means the target.
 */
public class Data implements Serializable {
    public double x,y;

    public Data() {};

    public Data(double x, double y){
        this.x = x;
        this.y = y;
    }

    @Override
    public String toString() {
        return "(" + x + "|" + y + ")";
    }

}
