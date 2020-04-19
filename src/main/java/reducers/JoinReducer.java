package reducers;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.AirlineKpiWritable;

import java.io.IOException;

public class JoinReducer extends Reducer<Text, AirlineKpiWritable, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<AirlineKpiWritable> values, Context context) throws IOException, InterruptedException {
        String airline = "";
        double kpi = 0;

        for (AirlineKpiWritable entry : values) {
            if (!entry.getAirline().toString().equals("")) {
                airline = entry.getAirline().toString();
            }

            if (entry.getKpi().get() != 0) {
                kpi = entry.getKpi().get();
            }
        }

        context.write(new Text(airline), new DoubleWritable(kpi));
    }
}
