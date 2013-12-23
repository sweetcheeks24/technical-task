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
 * Time: 8:20 PM
 * To change this template use File | Settings | File Templates.
 */

public class FindMissingMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
        String line = value.toString();
        System.out.println(line);

        String[] items = line.split("\\W");

        word.set(items[0]);
        context.write(word, one);
    }
}