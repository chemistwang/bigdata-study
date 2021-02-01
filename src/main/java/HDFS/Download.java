package HDFS;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @ClassName: Download
 * @Description: 下载
 * @Author: wyl
 * @Date: 2021-02-01 15:04
 * @Version 0.1
 **/
public class Download {


    @Test
    public void downLoad() throws IOException, URISyntaxException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.0.242:8020/"), new Configuration(), "root");
        //输入流
        FSDataInputStream inputStream = fileSystem.open(new Path("/herin/out.txt"));
        //输出流
        FileOutputStream outputStream = new FileOutputStream(new File("/Users/yagao/Desktop/xxx.txt"));
        //文件拷贝
        IOUtils.copy(inputStream, outputStream);
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);
        fileSystem.close();
    }


    @Test
    public void downLoad2() throws URISyntaxException, IOException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.0.242:8020/"), new Configuration(), "root");
        fileSystem.copyToLocalFile(new Path("/herin/out.txt"), new Path("/Users/yagao/Desktop/xxx.txt"));
    }


}
