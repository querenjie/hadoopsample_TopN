package com.querenjie.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class FlowRunner {
    public void run() throws Exception {

        System.setProperty("hadoop.home.dir", "d:/hadoop-3.1.0");
        Configuration configuration = new Configuration();
        //先运行统计
        System.out.println("开始运行统计过程。。。。。。");
        String inputPath = "D:/test/hadoop_data/topn_data/in/inputFile1.txt";
        String outputPath = "D:/test/hadoop_data/topn_data/out/";
        String[] paths = new String[2];
        paths[0] = inputPath;
        paths[1] = outputPath;
        System.out.println("原始文件的路径：paths[0] = " + paths[0]);
        System.out.println("统计结果文件的路径：paths[1] = " + paths[1]);
        long beginTime = System.currentTimeMillis();

        int run = ToolRunner.run(configuration, new FlowSumRunner(), paths);

        long endTime = System.currentTimeMillis();
        System.out.println("统计过程运行结束，用时" + (endTime - beginTime) + "毫秒");


        //然后运行排序
        System.out.println("开始运行排序过程。。。。。。");
        inputPath = "D:/test/hadoop_data/topn_data/out/part-r-00000";
        outputPath = "D:/test/hadoop_data/topn_data/out/sorted/";
        paths = new String[2];
        paths[0] = inputPath;
        paths[1] = outputPath;
        System.out.println("统计结果文件的路径：paths[0] = " + paths[0]);
        System.out.println("排序结果文件的路径：paths[1] = " + paths[1]);
        long beginTime2 = System.currentTimeMillis();

        run = ToolRunner.run(configuration, new FlowSumSortRunner(), paths);

        long endTime2 = System.currentTimeMillis();
        System.out.println("排序过程运行结束，用时" + (endTime2 - beginTime2) + "毫秒");
        System.out.println("运行结束，用时" + (endTime2 - beginTime) + "毫秒");

    }

    public static void main(String[] args) throws Exception {
        new FlowRunner().run();
    }
}
