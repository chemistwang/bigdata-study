package HDFS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @ClassName: Upload
 * @Description: 上传
 * @Author: wyl
 * @Date: 2021-02-01 15:15
 * @Version 0.1
 **/
public class Upload {


    @Test
    public void upload() throws URISyntaxException, IOException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.0.242:8020/"), new Configuration(), "root");
        fileSystem.copyFromLocalFile(new Path("/Users/yagao/Desktop/xxx.txt"), new Path("/herin/out.txt"));
        fileSystem.close();
    }


}
