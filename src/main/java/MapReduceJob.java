import mappers.FlightMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import reducers.FlightReducer;

import java.io.IOException;

public class MapReduceJob {

    public void start(String csvFilePath, String outputDirectory) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration, "BigDataProject - MapReduce");

        job.setJarByClass(this.getClass());
        job.setMapperClass(FlightMapper.class);
        job.setCombinerClass(FlightReducer.class);
        job.setReducerClass(FlightReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path directory = new Path(outputDirectory + "/mr-output/");

        FileInputFormat.addInputPath(job, new Path(csvFilePath));
        FileOutputFormat.setOutputPath(job, directory);

        // Delete output if exists// Delete output if exists
        FileSystem hdfs = FileSystem.get(configuration);
        if (hdfs.exists(directory)) hdfs.delete(directory, true);


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
