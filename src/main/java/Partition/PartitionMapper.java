package Partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName: PartitionMapper
 * @Description: TODO
 * @Author: wyl
 * @Date: 2021-02-02 11:00
 * @Version 0.1
 **/
public class PartitionMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    public enum Counter {
        MY_INPUT_RECORDS
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        /**
         * 计数器
         */
        //方法一：
//        Counter counter = context.getCounter("MY_COUNTER", "partition_counter");
//        //每次执行该方法，计数器变量值+1
//        counter.increment(1L);

        //方法二：
        context.getCounter(Counter.MY_INPUT_RECORDS).increment(1L);


        context.write(value, NullWritable.get());
    }
}
