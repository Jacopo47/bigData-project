package combiners;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.FlightDataWritable;

import java.io.IOException;

public class FlightCombiner extends Reducer<Text, FlightDataWritable, Text, FlightDataWritable> {
    @Override
    protected void reduce(Text key, Iterable<FlightDataWritable> values, Context context) throws IOException, InterruptedException {
        int totalArrivalDelay = 0;
        int totalDistance = 0;

        for (FlightDataWritable val : values) {
            totalArrivalDelay += val.getArrivalDelay().get();
            totalDistance += val.getDistance().get();
        }

        context.write(key, new FlightDataWritable(new IntWritable(totalArrivalDelay), new IntWritable(totalDistance)));
    }
}