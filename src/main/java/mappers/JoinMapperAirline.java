package mappers;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.AirlineKpiWritable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class JoinMapperAirline extends Mapper<Object, Text, Text, AirlineKpiWritable> {
    private static List<String> IATA_EXCLUDED = Collections.singletonList("IATA_CODE");
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            List<String> entry = Arrays.asList(value.toString().split(","));
            String iataCode = entry.get(0);
            String airline = entry.get(1);

            if (!IATA_EXCLUDED.contains(iataCode)) {
                context.write(new Text(iataCode), new AirlineKpiWritable(new Text(airline), new DoubleWritable(0)));
            }
        } catch (Exception ex) {
            context.write(new Text(ex.getMessage()), new AirlineKpiWritable(new Text(ex.getMessage()), new DoubleWritable(1)));
        }
    }
}