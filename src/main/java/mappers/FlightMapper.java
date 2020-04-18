package mappers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class FlightMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text word = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer iterator = new StringTokenizer(value.toString());
        while (iterator.hasMoreTokens()) {
            word.set(iterator.nextToken());
            Text returnKey = new Text(word.toString().substring(0, 1).toLowerCase());
            IntWritable returnValue = new IntWritable(word.getLength());
            context.write(returnKey, returnValue);
        }

    }
}
