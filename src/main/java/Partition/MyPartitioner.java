package Partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @ClassName: MyPartitioner
 * @Description: TODO
 * @Author: wyl
 * @Date: 2021-02-02 11:04
 * @Version 0.1
 **/
public class MyPartitioner extends Partitioner<Text, NullWritable> {

    /**
     * 1. 定义分区规则
     * 2. 返回对应的分区编号
     * @param text
     * @param nullWritable
     * @param i
     * @return
     */
    @Override
    public int getPartition(Text text, NullWritable nullWritable, int i) {

        String str = text.toString();
        if (Integer.parseInt(str) >= 300) {
            return 1;
        } else {
            return 0;
        }

    }
}
