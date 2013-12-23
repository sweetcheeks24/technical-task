package main.java.com.example.mapreduce.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: wendybartlett
 * Date: 12/22/13
 * Time: 6:08 PM
 * Technical task reducer
 */
public class SortReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // Variable to track the number of duplicates
        int sum = 0;
        // Aggregate values to calculate the number of duplicates
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(key, new IntWritable(sum));
    }

}
