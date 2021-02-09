package HBase;

/**
 * @ClassName: DDL
 * @Description: TODO
 * @Author: wyl
 * @Date: 2021-02-08 14:47
 * @Version 0.1
 **/


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.*;

/**
 * 1. 判断表是否存在
 * 2. 创建表
 * 3. 创建命名空间
 * 4. 删除表
 */
public class DDL {

    private static Connection connection = null;
    private static Admin admin = null;

    static {
        //创建连接
        try {
            //获取配置文件信息
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", "192.168.0.241");
            configuration.set("hbase.zookeeper.property.clientPort", "2181");
            connection = ConnectionFactory.createConnection(configuration);

            //获取客户端
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //关闭资源
    public static void close(){
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    //1. 判断表是否存在
    public static boolean isTableExist(String tableName) throws IOException {
        boolean b = admin.tableExists(TableName.valueOf(tableName));
        return b;
    }

    //2. 创建表
    public static void createTable(String tableName, String... cfs) throws IOException {

        //step1: 判断是否存在列族信息
        if (cfs.length <= 0) {
            System.out.println("请设置列族信息！");
            return;
        }

        //step2: 判断表是否存在
        if (isTableExist(tableName)) {
            System.out.println(tableName + "表已存在");
            return;
        }

        //step3: 创建表描述器
        List<ColumnFamilyDescriptor> columnFamilyDescriptorList = new ArrayList<ColumnFamilyDescriptor>();
        for (String cf:cfs) {
            columnFamilyDescriptorList.add(ColumnFamilyDescriptorBuilder.newBuilder(cf.getBytes()).build());
        }

        //step4: 创建表
        admin.createTable(TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName)).setColumnFamilies(columnFamilyDescriptorList).build());
    }

    //3. 删除表
    public static void dropTable(String tableName) throws IOException {

        //step1: 判断表是否存在
        boolean tableExist = isTableExist(tableName);
        if (!tableExist) {
            System.out.println(tableName + "表不存在");
            return;
        }
        //step2: 使表下线
        admin.disableTable(TableName.valueOf(tableName));

        //step3: 删除表
        admin.deleteTable(TableName.valueOf(tableName));
    }

    //4. 创建命名空间
    public static void createNameSpace(String ns) throws IOException {
        //step1: 创建命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(ns).build();
        //step2: 创建命名空间
        admin.createNamespace(namespaceDescriptor);
    }


    public static void main(String[] args) throws IOException {

        //Test
        boolean student = isTableExist( "xxx");
        //Test
        createTable("xxx", "info1", "info2");
        //Test
        dropTable("xxx1");
        //Test
        createNameSpace("wyl");

        //关闭资源
        close();
    }



}
