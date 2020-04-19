import combiners.FlightCombiner;
import mappers.FlightMapper;
import mappers.JoinMapperAirline;
import mappers.JoinMapperKpi;
import mappers.SortMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import reducers.FlightReducer;
import reducers.JoinReducer;
import reducers.SortReducer;
import utils.AirlineKpiWritable;
import utils.FlightDataWritable;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class MapReduceJob {
    private final static String FULL_PATH_FILE_AIRLINES = "hdfs:/user/jriciputi/bigdata/dataset/airlines.csv";

    public void start(String csvFilePath, String outputDirectory) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        List<Job> jobs = new LinkedList<>();

        String outputDirectoryMainJob = outputDirectory + "/mr-output";
        jobs.add(createMainJob(configuration, csvFilePath, outputDirectoryMainJob));

        String outputDirectorySortJob = outputDirectoryMainJob + "/sorted";
        jobs.add(createSortJob(configuration, outputDirectoryMainJob, outputDirectorySortJob));

        jobs.add(createJoinSort(configuration, outputDirectorySortJob, outputDirectory + "/final"));

        for (Job job : jobs) {
            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }
        }
    }

    private Job createMainJob(Configuration configuration, String csvFilePath, String outputDirectory) throws IOException {
        Job job = Job.getInstance(configuration, "BigDataProject - MapReduce Main");

        job.setJarByClass(this.getClass());
        job.setMapperClass(FlightMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlightDataWritable.class);
        job.setCombinerClass(FlightCombiner.class);
        job.setReducerClass(FlightReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setNumReduceTasks(1);

        Path outputDirectoryPath = new Path(outputDirectory);

        FileInputFormat.addInputPath(job, new Path(csvFilePath));
        FileOutputFormat.setOutputPath(job, outputDirectoryPath);

        // Delete output if exists// Delete output if exists
        FileSystem hdfs = FileSystem.get(configuration);
        if (hdfs.exists(outputDirectoryPath)) hdfs.delete(outputDirectoryPath, true);

        return job;
    }

    private Job createSortJob(Configuration configuration, String inputDirectory, String outputDirectory) throws IOException {
        Job job = Job.getInstance(configuration, "BigDataProject - MapReduce Sort");

        job.setJarByClass(this.getClass());
        job.setMapperClass(SortMapper.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setNumReduceTasks(1);

        Path outputDirectoryPath = new Path(outputDirectory);
        FileInputFormat.addInputPath(job, new Path(inputDirectory));
        FileOutputFormat.setOutputPath(job, outputDirectoryPath);

        // Delete output if exists// Delete output if exists
        FileSystem hdfs = FileSystem.get(configuration);
        if (hdfs.exists(outputDirectoryPath)) hdfs.delete(outputDirectoryPath, true);

        return job;
    }

    private Job createJoinSort(Configuration configuration, String inputDirectory, String outputDirectory) throws IOException {
        Job job = Job.getInstance(configuration, "BigDataProject - MapReduce Join");

        job.setJarByClass(this.getClass());
        job.setMapperClass(SortMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AirlineKpiWritable.class);
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setNumReduceTasks(1);

        MultipleInputs.addInputPath(job, new Path(inputDirectory), TextInputFormat.class, JoinMapperKpi.class);
        MultipleInputs.addInputPath(job, new Path(FULL_PATH_FILE_AIRLINES), TextInputFormat.class, JoinMapperAirline.class);

        Path outputDirectoryPath = new Path(outputDirectory);
        FileOutputFormat.setOutputPath(job, outputDirectoryPath);

        // Delete output if exists// Delete output if exists
        FileSystem hdfs = FileSystem.get(configuration);
        if (hdfs.exists(outputDirectoryPath)) hdfs.delete(outputDirectoryPath, true);

        return job;
    }
}
