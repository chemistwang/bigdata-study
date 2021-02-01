package HDFS;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @ClassName: Merge
 * @Description: 上传时合并小文件
 * @Author: wyl
 * @Date: 2021-02-01 15:32
 * @Version 0.1
 **/
public class Merge {

    @Test
    public void mergeFile () throws IOException, URISyntaxException, InterruptedException {

        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.0.242:8020/"), new Configuration(), "root");
        FSDataOutputStream outputStream = fileSystem.create(new Path("/bigFile.txt"));

        //获取本地文件系统
        LocalFileSystem local = FileSystem.getLocal(new Configuration());

        //通过本地文件系统获取文件列表，为一个集合
        FileStatus[] fileStatuses = local.listStatus(new Path("/Users/yagao/Desktop/txt"));

        for (FileStatus fileStatus:fileStatuses) {
            FSDataInputStream inputStream = local.open(fileStatus.getPath());
            IOUtils.copy(inputStream, outputStream);
            IOUtils.closeQuietly(inputStream);
        }

        IOUtils.closeQuietly(outputStream);
        local.close();
        fileSystem.close();

    }


}
