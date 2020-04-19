package mappers;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.AirlineKpiWritable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class JoinMapperAirline extends Mapper<Object, Text, Text, AirlineKpiWritable> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            List<String> entry = Arrays.asList(value.toString().split("\t"));

            String iataCode = entry.get(0);
            double kpi = Double.parseDouble(entry.get(1));

            context.write(new Text(iataCode), new AirlineKpiWritable(new Text(""), new DoubleWritable(kpi)));
        } catch (Exception ex) {
            context.write(new Text("ERROR"), new AirlineKpiWritable(new Text(ex.getMessage()), new DoubleWritable(0)));
        }
    }
}