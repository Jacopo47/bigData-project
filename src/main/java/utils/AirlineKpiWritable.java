package utils;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirlineKpiWritable implements Writable {
    private Text airline;
    private DoubleWritable kpi;

    public AirlineKpiWritable() {
        this.airline = new Text();
        this.kpi = new DoubleWritable(0);
    }

    public AirlineKpiWritable(Text airline, DoubleWritable kpi) {
        this.airline = airline;
        this.kpi = kpi;
    }

    public Text getAirline() {
        return airline;
    }

    public void setAirline(Text airline) {
        this.airline = airline;
    }

    public DoubleWritable getKpi() {
        return kpi;
    }

    public void setKpi(DoubleWritable kpi) {
        this.kpi = kpi;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        getAirline().write(out);
        getKpi().write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        getAirline().readFields(in);
        getKpi().readFields(in);
    }

    @Override
    public String toString() {
        return getAirline().toString() + "\t" + getKpi().toString();
    }
}