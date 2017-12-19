package com.hadoop.mr.v2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class SalesOrderDataRecord implements Writable {

    public IntWritable orderQty = new IntWritable();
    public DoubleWritable lineTotal = new DoubleWritable();
    
    public SalesOrderDataRecord() {
    }

    public SalesOrderDataRecord(int orderQty, double lineTotal) {
        this.orderQty.set(orderQty);
        this.lineTotal.set(lineTotal);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderQty.readFields(in);
        this.lineTotal.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.orderQty.write(out);
        this.lineTotal.write(out);
    }
    
    
    
}
