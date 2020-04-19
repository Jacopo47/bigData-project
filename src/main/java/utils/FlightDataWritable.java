package utils;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlightDataWritable implements Writable {
    private IntWritable arrivalDelay;
    private IntWritable distance;

    public FlightDataWritable() {
        this.arrivalDelay = new IntWritable(0);
        this.distance = new IntWritable(0);
    }

    public FlightDataWritable(IntWritable arrivalDelay, IntWritable distance) {
        this.arrivalDelay = arrivalDelay;
        this.distance = distance;
    }

    public IntWritable getArrivalDelay() {
        return arrivalDelay;
    }

    public void setArrivalDelay(IntWritable arrivalDelay) {
        this.arrivalDelay = arrivalDelay;
    }

    public IntWritable getDistance() {
        return distance;
    }

    public void setDistance(IntWritable distance) {
        this.distance = distance;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        getArrivalDelay().write(out);
        getDistance().write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        getArrivalDelay().readFields(in);
        getDistance().readFields(in);
    }

    @Override
    public String toString() {
        return getArrivalDelay().toString() + "\t" + getDistance().toString();
    }
}
