package reducers;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlightReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        double length = 0;
        double total = 0;
        for (IntWritable val : values) {
            total += val.get();
            length++;
        }

        context.write(key, new DoubleWritable(total / length));
    }
}
