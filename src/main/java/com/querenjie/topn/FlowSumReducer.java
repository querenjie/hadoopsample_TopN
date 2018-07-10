package com.querenjie.topn;

import com.querenjie.topn.beans.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println("------------------------come from reduce---------------------");
    }

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sumUpFlow = 0;
        long sumDownFlow = 0;
        for (FlowBean bean : values) {
            sumUpFlow += bean.getUpFlow();
            sumDownFlow += bean.getDownFlow();
        }
        System.out.println(key.toString() + "\t" + sumUpFlow + "\t" + sumDownFlow);
        context.write(key, new FlowBean(sumUpFlow, sumDownFlow));
    }
}
