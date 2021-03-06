package reducers;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.FlightDataWritable;

import java.io.IOException;

public class FlightReducer extends Reducer<Text, FlightDataWritable, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<FlightDataWritable> values, Context context) throws IOException, InterruptedException {
        double totalArrivalDelay = 0;
        double totalDistance = 0;

        for (FlightDataWritable val : values) {
            totalArrivalDelay += val.getArrivalDelay().get();
            totalDistance += val.getDistance().get();
        }

        context.write(key, new DoubleWritable(totalArrivalDelay / totalDistance));
    }
}
