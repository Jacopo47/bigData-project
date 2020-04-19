package mappers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.Flight;
import utils.FlightDataWritable;

import java.io.IOException;

public class FlightMapper extends Mapper<Object, Text, Text, FlightDataWritable> {
    private Text airline = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] rows = value.toString().split("\n");

        for (String row : rows) {
            try {
                Flight flight = Flight.extract(row);

                if (!flight.isCancelled() && flight.getArrivalDelay() > 0) {
                    airline.set(flight.getIataCode());

                    IntWritable arrivalDelay = new IntWritable(flight.getArrivalDelay());
                    IntWritable distance = new IntWritable(flight.getDistance());

                    FlightDataWritable flightData = new FlightDataWritable(arrivalDelay, distance);
                    context.write(airline, flightData);
                }
            } catch (Exception ex) {
                System.out.println("Error during flight mapping. " + ex.getMessage());
            }
        }
    }
}

