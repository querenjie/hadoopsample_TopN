package com.querenjie.topn;

import com.querenjie.topn.beans.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;

public class FlowSumRunner extends Configured implements Tool {

    public int run(String[] paths) throws Exception {
        Job job = Job.getInstance(this.getConf());
        job.setJarByClass(FlowSumRunner.class);
        job.setMapperClass(FlowSumMapper.class);
        job.setReducerClass(FlowSumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
//        FileInputFormat.setInputPaths(job, new Path(paths[0]));
        FileInputFormat.setInputPaths(job, paths[0]);
        FileOutputFormat.setOutputPath(job, new Path(paths[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "d:/hadoop-3.1.0");
//        String inputPath = "D:\\test\\hadoop_data\\topn_data\\in";
        String inputPath = "D:/test/hadoop_data/topn_data/in/inputFile1.txt";
        String outputPath = "D:/test/hadoop_data/topn_data/out/";
        String[] paths = new String[2];
        paths[0] = inputPath;
        paths[1] = outputPath;
        System.out.println("paths[0] = " + paths[0]);
        System.out.println("paths[1] = " + paths[1]);
        long beginTime = System.currentTimeMillis();

        int run = ToolRunner.run(new Configuration(), new FlowSumRunner(), paths);

        long endTime = System.currentTimeMillis();
        System.out.println("运行结束，用时" + (endTime - beginTime) + "毫秒");

        System.exit(run);
    }
}
