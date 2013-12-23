package com.example.mapreduce.sort;

import main.java.com.example.mapreduce.sort.SortMapper;
import main.java.com.example.mapreduce.sort.SortReducer;
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
 * Time: 7:01 PM
 * To change this template use File | Settings | File Templates.
 */

public class TestSort {
    MapDriver<LongWritable, Text, Text, IntWritable>                            mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable>                          reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>   mapReduceDriver;

    @Before
    public void setUp() {
        // Set up the mapper test harness
        SortMapper mapper = new SortMapper();
        mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
        mapDriver.setMapper(mapper);

        // Set up the reducer test harness
        SortReducer reducer = new SortReducer();
        reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>(reducer);
        reduceDriver.setReducer(reducer);

        mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);
        mapReduceDriver.resetOutput();
    }

    @Test
    public void testMapper() throws IOException {
        //input = 1, 10 30 20 10
        mapDriver.withInput(new LongWritable(1), new Text("10 30 20 10"));
        //expected output = {"10", 1}, {"20", 1}, {"30", 1}
        mapDriver.withOutput(new Text("10"), new IntWritable(1));
        mapDriver.withOutput(new Text("30"), new IntWritable(1));
        mapDriver.withOutput(new Text("20"), new IntWritable(1));
        mapDriver.withOutput(new Text("10"), new IntWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        //input = {"10", [1, 1]}
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver.withInput(new Text("10"), values);
        // expected output = {"10", 2}
        reduceDriver.withOutput(new Text("10"), new IntWritable(2));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        // input = 1, 10 30 20 10
        mapReduceDriver.withInput(new LongWritable(1), new Text("10 30 20 10"));
        // expected output = {"10", 2}, {"20", 1}, {"30", 1}
        mapReduceDriver.addOutput(new Text("10"), new IntWritable(2));
        mapReduceDriver.addOutput(new Text("20"), new IntWritable(1));
        mapReduceDriver.addOutput(new Text("30"), new IntWritable(1));
        mapReduceDriver.runTest();
    }

}
