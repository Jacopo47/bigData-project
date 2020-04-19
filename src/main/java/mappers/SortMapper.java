package mappers;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;


public class SortMapper extends Mapper<Object, Text, DoubleWritable, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            List<String> entry = Arrays.asList(value.toString().split("\t"));

            String iataCode = entry.get(0);
            double kpi = Double.parseDouble(entry.get(1));

            context.write(new DoubleWritable(kpi), new Text(iataCode));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
