package MapReduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName: WordCountReduce
 * @Description: 自定义reduce方法
 * @Author: wyl
 * @Date: 2021-02-01 17:59
 * @Version 0.1
 **/

/**
 * 四个泛型解释：
 * KEYIN: K2类型
 * VALUEIN: V2类型
 * KEYOUT: K3类型
 * VALUEOUT: V3类型
 */
public class WordCountReduce extends Reducer<Text, LongWritable, Text, LongWritable> {

    /**
     * 将K2和V2转为K3和V3，将K3和V3写入上下文中
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0;
        for(LongWritable value : values) {
            count += value.get();
        }
        context.write(key, new LongWritable(count));
    }
}
