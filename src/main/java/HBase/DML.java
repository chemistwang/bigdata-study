package HBase;

/**
 * @ClassName: DML
 * @Description: TODO
 * @Author: wyl
 * @Date: 2021-02-08 14:47
 * @Version 0.1
 **/

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 1. 插入数据
 * 2. 查数据（get）
 * 3. 查数据（scan）
 * 4. 删除数据
 */
public class DML {

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

    //1. 向表插入数据
    public static void putData(String tableName, String rowKey, String cf, String cn, String value) throws IOException {
        //step1: 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        //step2: 创建put对象
        Put put = new Put(Bytes.toBytes(rowKey));
        //step3: 给put对象赋值
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(value));
        //step4: 插入数据
        table.put(put);
        //step5: 关闭表连接
        table.close();
    }

    //2. 获取数据（get）
    public static void getData(String tableName, String rowKey, String cf, String cn) throws IOException {
        //step1: 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        //step2: 创建Get对象
        Get get = new Get(Bytes.toBytes(rowKey));
        //step3: 获取数据
        Result result = table.get(get);
        //step4: 解析result并打印
        for(Cell cell:result.rawCells()) {

            //step5: 打印数据
            System.out.println("CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) + ",CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) + ",Value:" + Bytes.toString(CellUtil.cloneValue(cell)));

        }
        //step5: 关闭表连接
        table.close();
    }



    public static void main(String[] args) throws IOException {
        //Test
//        putData("student", "10001", "info", "age", "24");
        //Test
        getData("student", "10001", "info", "age");

    }


}
