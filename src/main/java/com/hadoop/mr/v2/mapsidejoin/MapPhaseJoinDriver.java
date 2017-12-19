package com.hadoop.mr.v2.mapsidejoin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
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

import com.hadoop.mr.v2.reducejoin.JoinGenericWritable;
import com.hadoop.mr.v2.reducejoin.ProductIdKey;
import com.hadoop.mr.v2.reducejoin.ReducePhaseJoinDriver;
import com.hadoop.mr.v2.reducejoin.ReducePhaseJoinDriver.JoinGroupingComparator;
import com.hadoop.mr.v2.reducejoin.ReducePhaseJoinDriver.JoinSortingComparator;
import com.hadoop.mr.v2.reducejoin.ReducePhaseJoinDriver.SalesOrderDataMapper;
import com.hadoop.mr.v2.reducejoin.SalesOrderDataRecord;

/**
 * select P.ProductID, P.Name, P.ProductNumber, C.Name, Sum(S.OrderQty),
 * Sum(S.LineTotal) from SalesOrderDetails S join Products P on S.ProductID =
 * P.ProductID join ProductSubCategory C on P.ProductSubcategoryId =
 * C.ProductSubcategoryId where S.OrderQty > 0
 *
 */
public class MapPhaseJoinDriver extends Configured implements Tool {

    public static class ProductMapper extends Mapper<LongWritable, Text, ProductIdKey, JoinGenericWritable> {

        private static final HashMap<Integer, String> productSubCategories = new HashMap<Integer, String>();

        @Override
        protected void setup(Mapper<LongWritable, Text, ProductIdKey, JoinGenericWritable>.Context context)
                throws IOException, InterruptedException {
            try (Job job = Job.getInstance(context.getConfiguration())) {
                URI[] uris = context.getCacheFiles();
                FSDataInputStream dataIn = FileSystem.get(context.getConfiguration()).open(new Path(uris[0]));
                try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(dataIn))) {
                    String line = bufferedReader.readLine();
                    while (line != null) {
                        String[] recordFields = line.split("\\t");
                        int key = Integer.parseInt(recordFields[0]);
                        String productSubcategoryName = recordFields[2];
                        productSubCategories.put(key, productSubcategoryName);
                        line = bufferedReader.readLine();
                    }
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, ProductIdKey, JoinGenericWritable>.Context context)
                throws IOException, InterruptedException {
            String[] recordFields = value.toString().split("\\t");
            int productId = Integer.parseInt(recordFields[0]);
            int productSubcategoryId = recordFields[18].length() > 0 ? Integer.parseInt(recordFields[18]) : 0;
            String productName = recordFields[1];
            String productNumber = recordFields[2];
            String productSubcategoryName = productSubcategoryId > 0 ? productSubCategories.get(productSubcategoryId) : "";

            ProductIdKey recordKey = new ProductIdKey(productId, ProductIdKey.PRODUCT_RECORD);
            ProductRecord record = new ProductRecord(productName, productNumber, productSubcategoryName);

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
                    output.append(record.productSubCategoryName.toString()).append(", ");
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
    public int run(String[] arr) throws Exception {
        Configuration conf = getConf();
        conf.set("dfs.replication", "1");
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        conf.set("fs.defaultFS", "hdfs://0.0.0.0:19000");

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

            job.addCacheFile(new Path(args[2]).toUri());

            Path outputPath = new Path(args[3]);

            FileSystem fs = FileSystem.get(outputPath.toUri(), conf);
            if (fs.exists(outputPath))
                fs.delete(outputPath, true);
            fs.close();

            FileOutputFormat.setOutputPath(job, outputPath);
            if (job.waitForCompletion(true)) {
                return 0;
            } else {
                return 1;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new MapPhaseJoinDriver(), args);
    }

}
