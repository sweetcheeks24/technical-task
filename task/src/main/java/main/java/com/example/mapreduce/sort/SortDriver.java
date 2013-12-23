package main.java.com.example.mapreduce.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created with IntelliJ IDEA.
 * User: wendybartlett
 * Date: 12/22/13
 * Time: 6:08 PM
 * Task:

 You are given a very very large list of unsorted integers. These integers are supposed to be unique and, if sorted, contiguous. However, you suspect that this is not the case, so you want to write code to check for missing or duplicate integers. Write code to return these results:

 - Are there any missing or duplicate integers?
 - How many missing integers?
 - How many duplicate integers?
 - Which integers are missing?
 - Which integers are duplicates, and how many duplicates of each integer?

 You may write separate code or functions to answer each of these questions; i.e. it's ok to pass over the data more than once to answer all questions.

 Your implementation may be in any language/system. However, remember that this is big data, so the solution needs to useable at that scale.

 Please spend no more than 3 hours. If you'd like to spend more, it's up to you - just make a note of when you get to three and report back as to when you restarted the clock ."

 Feel free to ask any follow up questions and let us know when you believe you will be able to get it back to us.

*/

public class SortDriver extends Configured implements Tool {
    private static final String JOB_NAME = "Wikimedia Technical Task";

    @Override
    public int run(String[] strings) throws Exception {
        if (strings.length != 2) {
            System.out.printf("Usage: %s [generic options] <input_dir> <output_dir>\n", getClass().getSimpleName());
            return -1;
        }
        Configuration config = new Configuration();
        String[] otherArgs = new GenericOptionsParser(config, strings).getRemainingArgs();

        Job job = Job.getInstance(config, JOB_NAME);
        job.setJarByClass(SortDriver.class);
        job.setMapperClass(SortMapper.class);
        job.setCombinerClass(SortReducer.class);
        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        String initial_output_path = otherArgs[1] + '-' + String.valueOf(System.currentTimeMillis());
        FileOutputFormat.setOutputPath(job, new Path(initial_output_path));

        job.submit();

        while(!job.isComplete()) {
            Thread.sleep(2000);
        }

        Job find_missing_job = Job.getInstance(config, JOB_NAME);
        // Define new mapper
        find_missing_job.setMapperClass(FindMissingMapper.class);
        // Here we can reuse the same reducer
        find_missing_job.setCombinerClass(FindMissingReducer.class);
        find_missing_job.setReducerClass(FindMissingReducer.class);
        find_missing_job.setOutputKeyClass(Text.class);
        find_missing_job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(initial_output_path));
        FileOutputFormat.setOutputPath(job, new Path(initial_output_path + '-' + System.currentTimeMillis()));

        return  (find_missing_job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new SortDriver(), args);
        System.exit(exitCode);
    }
}
