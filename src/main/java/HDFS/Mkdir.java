package HDFS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @ClassName: Mkdir
 * @Description: 创建文件夹
 * @Author: wyl
 * @Date: 2021-02-01 14:55
 * @Version 0.1
 **/
public class Mkdir {

    @Test
    public void mkdir() throws IOException, URISyntaxException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.0.242:8020/"), new Configuration(), "root");
        boolean mkdirs = fileSystem.mkdirs(new Path("/p/a/t/h"));
        fileSystem.close();
    }

}
