package HDFS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @ClassName: ListFileSystem
 * @Description: 遍历文件系统
 * @Author: wyl
 * @Date: 2021-02-01 14:52
 * @Version 0.1
 **/
public class ListFileSystem {


    @Test
    public void listFileSystem() throws URISyntaxException, IOException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.0.242:8020/"), new Configuration(), "root");
        System.out.println(fileSystem + "fileSystem");
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(new Path("/"), true);
        while (locatedFileStatusRemoteIterator.hasNext()) {
            LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
            System.out.println(next.getPath().toString());
        }
        fileSystem.close();
    }

}
