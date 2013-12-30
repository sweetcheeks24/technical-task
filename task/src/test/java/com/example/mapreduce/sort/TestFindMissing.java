package com.example.mapreduce.sort;

import main.java.com.example.mapreduce.sort.FindMissingMapper;
import main.java.com.example.mapreduce.sort.FindMissingReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: wendybartlett
 * Date: 12/22/13
 * Time: 8:23 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestFindMissing {
    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        // Set up the mapper test harness
        FindMissingMapper mapper = new FindMissingMapper();
        mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
        mapDriver.setMapper(mapper);

        // Set up the reducer test harness
        FindMissingReducer reducer = new FindMissingReducer();
        reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>(reducer);
        reduceDriver.setReducer(reducer);

        mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);
        mapReduceDriver.resetOutput();
    }

    @Test
    public void testMapper() throws IOException {
        //input = 1, 2 10
        mapDriver.withInput(new LongWritable(1), new Text("10 1\n13 10"));
        //expected output = {"10", 1}, {"13", 1}
        mapDriver.withOutput(new Text("10"), new IntWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        //input = {"10", [1]}, {"13", [1]}
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        reduceDriver.withInput(new Text("10"), values);
        reduceDriver.withInput(new Text("13"), values);
        // expected output = {"1", 1}, {"2", 1}, {"3", 1}, {"4", 1}, {"5", 1}, {"6", 1}, {"7", 1}, {"8", 1}, {"9", 1}, {"11", 1}, {"12", 1}
        reduceDriver.withOutput(new Text("1"), new IntWritable(1));
        reduceDriver.withOutput(new Text("2"), new IntWritable(1));
        reduceDriver.withOutput(new Text("3"), new IntWritable(1));
        reduceDriver.withOutput(new Text("4"), new IntWritable(1));
        reduceDriver.withOutput(new Text("5"), new IntWritable(1));
        reduceDriver.withOutput(new Text("6"), new IntWritable(1));
        reduceDriver.withOutput(new Text("7"), new IntWritable(1));
        reduceDriver.withOutput(new Text("8"), new IntWritable(1));
        reduceDriver.withOutput(new Text("9"), new IntWritable(1));
        reduceDriver.withOutput(new Text("11"), new IntWritable(1));
        reduceDriver.withOutput(new Text("12"), new IntWritable(1));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
//        System.out.println("Map/Reduce:");
//        // input = 1, 10 1 \n 13 10
        mapReduceDriver.withInput(new LongWritable(1), new Text("10 1"));
        mapReduceDriver.withInput(new LongWritable(1), new Text("13 10"));
//        // expected output = {"1", 1}, {"2", 1}, {"3", 1}, {"4", 1}, {"5", 1}, {"6", 1}, {"7", 1}, {"8", 1}, {"9", 1}, {"11", 1}, {"12", 1}
        mapReduceDriver.addOutput(new Text("1"), new IntWritable(1));
        mapReduceDriver.addOutput(new Text("2"), new IntWritable(1));
        mapReduceDriver.addOutput(new Text("3"), new IntWritable(1));
        mapReduceDriver.addOutput(new Text("4"), new IntWritable(1));
        mapReduceDriver.addOutput(new Text("5"), new IntWritable(1));
        mapReduceDriver.addOutput(new Text("6"), new IntWritable(1));
        mapReduceDriver.addOutput(new Text("7"), new IntWritable(1));
        mapReduceDriver.addOutput(new Text("8"), new IntWritable(1));
        mapReduceDriver.addOutput(new Text("9"), new IntWritable(1));
        mapReduceDriver.addOutput(new Text("11"), new IntWritable(1));
        mapReduceDriver.addOutput(new Text("12"), new IntWritable(1));

        mapReduceDriver.runTest();
    }
}
