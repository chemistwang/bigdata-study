package HDFS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @ClassName: AccessFileSystem
 * @Description: 访问文件系统
 * @Author: wyl
 * @Date: 2021-02-01 14:51
 * @Version 0.1
 **/
public class AccessFileSystem {

    @Test
    public void getFileSystem() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.0.242:8020/");
        FileSystem fileSystem = FileSystem.get(configuration);
        System.out.println(fileSystem.toString());
    }


    @Test
    public void getFileSystem2() throws URISyntaxException, IOException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.0.242:8020/"), new Configuration(), "root");
        System.out.println(fileSystem + "fileSystem");
    }


    @Test
    public void getFileSystem3() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.0.241:8020/");
        FileSystem fileSystem = FileSystem.newInstance(configuration);
        System.out.println(fileSystem.toString());
    }

    @Test
    public void getFileSystem4() throws URISyntaxException, IOException {
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://192.168.0.241:8020/"), new Configuration());
        System.out.println(fileSystem.toString());
    }

}
