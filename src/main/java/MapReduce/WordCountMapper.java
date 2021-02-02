package MapReduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName: WordCountMapper
 * @Description: 自定义mapper方法
 * @Author: wyl
 * @Date: 2021-02-01 17:56
 * @Version 0.1
 **/

/**
 * 四个泛型解释：
 * KEYIN: K1类型      Long    =>   LongWritable
 * VALUEIN: V1类型    String  =>   Text
 * KEYOUT: K2类型     String  =>   Text
 * VALUEOUT>: V2类型  Long    =>   LongWritable
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    Text text = new Text();
    LongWritable longWritable = new LongWritable();

    /**
     * 将K1，V1转为K2，V2
     * @param key   K1，行偏移量
     * @param value V1，每一行的文本数据
     * @param context 表示上下文对象，用来连接各个流程
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split(",");
        for(String word : split) {
            //context.write(word, 1); 需要做一个数据类型转化
            text.set(word);
            longWritable.set(1);
            context.write(text,longWritable);
        }
    }
}
