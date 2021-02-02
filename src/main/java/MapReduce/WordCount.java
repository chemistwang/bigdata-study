package MapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * @ClassName: WordCount
 * @Description: 利用MapReduce统计单词数量
 * @Author: wyl
 * @Date: 2021-02-01 17:49
 * @Version 0.1
 **/
public class WordCount extends Configured implements Tool {

    //指定一个job任务
    public int run(String[] strings) throws Exception {

        //创建一个job任务
        Job job = Job.getInstance(super.getConf(), WordCount.class.getSimpleName());

        //打包到集群上面运行的时候，必须要添加一下配置，指定程序的main函数
        job.setJarByClass(WordCount.class);

        //配置job任务对象（8个步骤）

        //第一步：指定文件的读取方式和读取路径，读取输入文件解析成key，value对
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("hdfs://192.168.0.242:8020/wyl/wordcount.txt"));

        //第二步：指定Map阶段的处理方式，设置自定义mapper类
        job.setMapperClass(WordCountMapper.class);
        //设置map阶段的K2类型
        job.setMapOutputKeyClass(Text.class);
        //设置Map阶段的V2类型
        job.setMapOutputValueClass(LongWritable.class);

        //第三、四、五、六步采用默认的方式

        //第七步：指定Reduce阶段的处理方式和数据类型
        job.setReducerClass(WordCountReduce.class);
        //设置K3类型
        job.setOutputKeyClass(Text.class);
        //设置V3类型
        job.setOutputValueClass(LongWritable.class);

        //第八步：设置输出类型
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置输出的路径
        Path path = new Path("hdfs://192.168.0.242:8020/wyl/wordCount_out");
        TextOutputFormat.setOutputPath(job, path);
        //若输出文件路径已存在则会报错，先行判断删除
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.0.242:8020"), new Configuration());
        //判断目录是否存在
        boolean exists = fileSystem.exists(path);
        if (exists) {
            //删除目标目录
            fileSystem.delete(path, true);
        }


        //等待任务结束
        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;

    }


    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        //启动job任务
        int run = ToolRunner.run(configuration, new WordCount(), args);
        System.exit(run);
    }


}
