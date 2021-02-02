package Combiner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName: MyCombiner
 * @Description: reduce之前先做一次combiner
 * @Author: wyl
 * @Date: 2021-02-02 15:36
 * @Version 0.1
 **/
public class MyCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0;
        for(LongWritable value : values) {
            count += value.get();
        }
        context.write(key, new LongWritable(count));
    }
}
