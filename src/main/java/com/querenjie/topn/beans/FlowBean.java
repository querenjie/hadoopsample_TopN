package com.querenjie.topn.beans;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 封装一个用于统计和排序流量的Bean
 */
public class FlowBean implements WritableComparable<FlowBean> {
    private long upFlow;
    private long downFlow;
    private long sumFlow;

    public FlowBean() {}

    public FlowBean(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = this.upFlow + this.downFlow;
    }

    public int compareTo(FlowBean o) {
        return this.sumFlow > o.sumFlow ? -1 : 1;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.upFlow);
        dataOutput.writeLong(this.downFlow);
        dataOutput.writeLong(this.sumFlow);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
        this.sumFlow = dataInput.readLong();
    }

    @Override
    public String toString() {
        return "" + upFlow +
                "\t" + downFlow +
                "\t" + sumFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }
}
