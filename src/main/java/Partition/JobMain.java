package Partition;

import MapReduce.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @ClassName: JobMain
 * @Description: TODO
 * @Author: wyl
 * @Date: 2021-02-02 11:19
 * @Version 0.1
 **/
public class JobMain extends Configured implements Tool {

    public int run(String[] strings) throws Exception {
        //1. 创建job任务对象
        Job job = Job.getInstance(super.getConf(), JobMain.class.getSimpleName());

        job.setJarByClass(JobMain.class);

        //2. 对job任务进行配置（8个步骤）

        //第一步：设置输入类和输入的路径
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("hdfs://192.168.0.242:8020/wyl/number.txt"));

        //第二步：设置Mapper类和数据类型
        job.setMapperClass(PartitionMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //第三步：指定分区类
        job.setPartitionerClass(MyPartitioner.class);

        //第四、五、六步

        //第七步：指定Reducer类和数据类型
        job.setReducerClass(PartitionReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //设置ReduceTask的个数
        job.setNumReduceTasks(2);

        //第八步：指定输出类和输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("hdfs://192.168.0.242:8020/wyl/number_out"));

        //3. 等待任务结束
        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        //启动job任务
        int run = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(run);
    }

}
