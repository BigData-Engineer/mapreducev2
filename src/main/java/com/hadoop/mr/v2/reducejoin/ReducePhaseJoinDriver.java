package com.hadoop.mr.v2.reducejoin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Implements below query logic in Map Reduce:: select P.ProductID, P.Name,
 * P.ProductNumber, Sum(S.OrderQty), Sum(S.LineTotal) from SalesOrderDetails S
 * join Products P on S.ProductID = P.ProductID where S.OrderQty > 0
 */
public class ReducePhaseJoinDriver extends Configured implements Tool {

    public static class JoinGroupingComparator extends WritableComparator {
        public JoinGroupingComparator() {
            super(ProductIdKey.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            ProductIdKey first = (ProductIdKey) a;
            ProductIdKey second = (ProductIdKey) b;
            return first.productId.compareTo(second.productId);
        }
    }

    public static class JoinSortingComparator extends WritableComparator {
        public JoinSortingComparator() {
            super(ProductIdKey.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            ProductIdKey first = (ProductIdKey) a;
            ProductIdKey second = (ProductIdKey) b;
            return first.compareTo(second);
        }
    }

    public static class SalesOrderDataMapper extends Mapper<LongWritable, Text, ProductIdKey, JoinGenericWritable> {
        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, ProductIdKey, JoinGenericWritable>.Context context)
                throws IOException, InterruptedException {
            String[] recordFields = value.toString().split("\\t");
            int productId = Integer.parseInt(recordFields[4]);
            int orderQty = Integer.parseInt(recordFields[3]);
            double lineTotal = Double.parseDouble(recordFields[8]);

            ProductIdKey recordKey = new ProductIdKey(productId, ProductIdKey.DATA_RECORD);
            SalesOrderDataRecord recordValue = new SalesOrderDataRecord(orderQty, lineTotal);

            JoinGenericWritable genericRecord = new JoinGenericWritable(recordValue);
            context.write(recordKey, genericRecord);
        }
    }

    public static class ProductMapper extends Mapper<LongWritable, Text, ProductIdKey, JoinGenericWritable> {
        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, ProductIdKey, JoinGenericWritable>.Context context)
                throws IOException, InterruptedException {
            String[] recordFields = value.toString().split("\\t");
            int productId = Integer.parseInt(recordFields[0]);
            String productName = recordFields[1];
            String productNumber = recordFields[2];

            ProductIdKey recordKey = new ProductIdKey(productId, ProductIdKey.PRODUCT_RECORD);
            ProductRecord record = new ProductRecord(productName, productNumber);

            JoinGenericWritable genericRecord = new JoinGenericWritable(record);
            context.write(recordKey, genericRecord);
        }
    }

    public static class JoinReducer extends Reducer<ProductIdKey, JoinGenericWritable, NullWritable, Text> {
        @Override
        protected void reduce(ProductIdKey key, Iterable<JoinGenericWritable> values,
                Reducer<ProductIdKey, JoinGenericWritable, NullWritable, Text>.Context context)
                throws IOException, InterruptedException {
            StringBuilder output = new StringBuilder();
            int sumOrderQty = 0;
            double sumLineTotal = 0.0;
            for (JoinGenericWritable value : values) {
                if (key.recordType.equals(ProductIdKey.PRODUCT_RECORD)) {
                    ProductRecord record = (ProductRecord) value.get();
                    output.append(key.productId.toString()).append(", ");
                    output.append(record.productName.toString()).append(", ");
                    output.append(record.productNumber.toString()).append(", ");
                } else {
                    SalesOrderDataRecord record = (SalesOrderDataRecord) value.get();
                    sumOrderQty += Integer.parseInt(record.orderQty.toString());
                    sumLineTotal += Double.parseDouble(record.lineTotal.toString());
                }
            }

            if (sumOrderQty > 0) {
                context.write(NullWritable.get(), new Text(output.toString() + sumOrderQty + ", " + sumLineTotal));
            }
        }
    }

    @Override
    public int run(String[] arr) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = getConf();
        conf.set("dfs.replication", "1");
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());

        String[] args = new GenericOptionsParser(conf, arr).getRemainingArgs();
        try (Job job = Job.getInstance(conf)) {
            job.setJarByClass(ReducePhaseJoinDriver.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setMapOutputKeyClass(ProductIdKey.class);
            job.setMapOutputValueClass(JoinGenericWritable.class);

            MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, SalesOrderDataMapper.class);
            MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ProductMapper.class);

            job.setReducerClass(JoinReducer.class);
            job.setSortComparatorClass(JoinSortingComparator.class);
            job.setGroupingComparatorClass(JoinGroupingComparator.class);

            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            FileSystem fs = DistributedFileSystem.get(new Path(args[2]).toUri(), conf);
            if (fs.exists(new Path(args[2])))
                fs.delete(new Path(args[2]), true);
            fs.close();
            
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            if (job.waitForCompletion(true)) {
                return 0;
            } else {
                return 1;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println(ToolRunner.run(new ReducePhaseJoinDriver(), args));
    }

}
