package main.java.com.example.mapreduce.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: wendybartlett
 * Date: 12/22/13
 * Time: 6:08 PM
 * Technical task mapper.
 */
public class SortMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
        String line = value.toString();
        String[] items = line.split("\\W");
        for (final String item : items) {
            word.set(item);
            context.write(word, one);
        }
    }

}
