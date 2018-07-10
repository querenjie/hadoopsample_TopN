package com.querenjie.topn;

import com.querenjie.topn.beans.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println("------------------come from map-------------------------");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String lineContent = value.toString();
        System.out.println(lineContent);
        String[] fields = lineContent.split("\t");
        String phoneNo = fields[0];
        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);
        context.write(new Text(phoneNo), new FlowBean(upFlow, downFlow));
    }
}
