package main.java.com.example.mapreduce.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: wendybartlett
 * Date: 12/22/13
 * Time: 9:15 PM
 * To change this template use File | Settings | File Templates.
 */
public class FindMissingReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private int pre_val = 1;
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // Variable to track the last value
        int last_value = Integer.parseInt(key.toString());
        System.out.println(pre_val + " : " + last_value);
        while (pre_val != last_value) {
            // Aggregate values to calculate the number of duplicates
            System.out.println("Previous:" + pre_val);

            // Identify which integers are missing (Part 4)
            context.write(new Text(String.valueOf(pre_val)), new IntWritable(1));
            pre_val += 1;

            // Track the number of missing integers (Part 2)
            context.getCounter("Integers", "Number-Missing").increment(1);
        }
        pre_val = last_value + 1;
    }

}