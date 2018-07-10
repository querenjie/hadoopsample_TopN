package com.querenjie.topn;

import com.querenjie.topn.beans.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowSumSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String lineContent = value.toString();
        String[] columns = lineContent.split("\t");
        String phoneNo = columns[0];
        long upFlow = Long.parseLong(columns[1]);
        long downFlow = Long.parseLong(columns[2]);
        FlowBean flowBean = new FlowBean(upFlow, downFlow);
        context.write(flowBean, new Text(phoneNo));
    }
}
