<center> <font color=#000000 size=6 face="黑体">大数据实验四  实验报告</font></center>

<center> <font color=#000000 size=4 face="宋体">白家杨 161220002   刘笑今 161220084   曹佳涵 161220003</font></center>

### 一、  实验任务

1. 在自己本地电脑上正确安装和运行HBase(推荐1.4.9)和Hive(推荐2.3.4)。

2. 在HBase中创建一张表Wuxia，用于保存下一步的输出结果。

3. 修改第3次实验的MapReduce程序，在Reduce阶段将倒排索引的信息通过文件输出， 而每个词语及其对应的“平均出现次数”信息写入到HBase的表“Wuxia”中。

4. 编写Java程序，遍历上一步中保存在HBase中的表，并把表格的内容(词语以及平 均出现次数)保存到本地文件中。

5. Hive安装完成后，在Hive Shell命令行操作创建表(表名:Wuxia(word STRING, count DOUBLE))、导入平均出现次数的数据、查询(出现次数大于300的词语)和 前100个出现次数最多的词。

### 二、实验环境说明

| JAVA 版本 | hadoop 版本  | HBase版本 | Hive版本 |
| :-------: | :----------: | :-------: | :------: |
| jdk-1.8.0 | hadoop-2.7.1 |   1.4.9   |  2.3.4   |

### 三、任务：修改MapReduce程序

### 四、任务：遍历HBase表并保存

在上一个任务中，已经将倒排索引的数据存储到了HBase的Wuxia表中，列族名为Content，列名为average num。这个任务主要就是遍历HBase中的Wuxia表，将表中的内容保存到本地文件中。

遍历HBase表主要使用到的操作是扫描（scan）技术，它类似于数据库系统中的游标（cursor），并利用到了HBase提供的底层顺序存储的数据结构。扫描操作的工作方式类似于迭代器，所以用户无需调用scan()方法创建实例，只需要调用HTable的`getScanner()`方法，此方法在返回真正的扫描器（scanner）实例的同时，用户也可以使用它迭代获取数据。

扫描操作不会通过一次RPC请求返回所有匹配的行，而是以行为单位进行返回。在本次实验中，行的数目很大，如果同时在一次请求中发送大量数据，会占用大量的系统资源并消耗很长时间。因此使用`ResultScanner`类把扫描操作转换为类似的`get`操作，它将每一行数据封装成一个`Result`实例，并将所有的`Result`实例放入一个迭代器中，通过`next()`方法进行遍历。

实际代码如下：

```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class ScanTable {
    public static void main(String[] args) throws IOException {
        // 创建所需的配置
        Configuration conf = HBaseConfiguration.create();
        // 实例化一个新的客户端
        HTable table = new HTable(conf, "Wuxia");
        // 创建一个空的Scan实例
        Scan scan = new Scan();
        // 将限制条件添加到Scan中
        scan.addColumn(Bytes.toBytes("Content"), Bytes.toBytes("average num"));
        // 取得一个扫描器迭代访问所有的行
        ResultScanner scanner = table.getScanner(scan);
        try {
            // 创建新文件
            File file = new File("result.txt");
            if (!file.exists()) {
                // 若文件已存在则重新创建
                file.createNewFile();
            }
            // 创建fileWriter
            FileWriter fileWriter = new FileWriter(file.getName());
            // 遍历每一行的数据
            for (Result res : scanner) {
                // 取出KeyValue类对象
                for(KeyValue kv : res.raw()){
                    // 获取Row值和Value值，并写入文件
                    fileWriter.write(Bytes.toString(kv.getRow()) + "\t");
                    fileWriter.write(Bytes.toDouble(kv.getValue()) + "\r\n");
                }
            }
            // 关闭fileWriter
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 关闭scanner
        scanner.close();
        // 关闭table
        table.close();
    }
}

```

在代码中，首先实例化一个HBase client，使用它来进行后续的api接口调用，在Scan实例中，需要指定列族名和列名，以保证只读入指定列族指定列的数据，通过扫描器迭代获取所有的行，将其存入`ResultScanner`实例中，并将其中的word和count数据取出，注意再上一个任务存入数据时word类型为`string`，count类型为`double`，因此在取出时，需要将取出的byte[]类型转换为相应的`string`和`double`，并将其写入文件。

### 五、任务：Hive Shell表操作

配置好Hive后，首先建立一个wuxia表：

```mysql
create table wuxia (word STRING, count DOUBLE) row format delimited fields terminated by '\t' stored as textfile;
```

建表指令中，规定了表的两个属性为word和count，类型分别为字符串和double。该表可以从文件加载进来，对文件格式的要求是：每行一个条目(word count)，中间用'\t'隔开。

根据实验前一部分得到的数据文件，将数据文件导入wuxia表：

```mysql
load data inpath "/home/hadoop/data/result.txt" into table wuxia;
```

再执行：

```mysql
select * from wuxia;
```

即可看到所有条目，说明加载成功。

查找平均出现次数大于300的词语：

```mysql
select word from wuxia where count > 300;
```

查找平均出现次数最大的100的词语：

```mysql
select word from wuxia sort by count desc limit 100;
```

### 六、运行结果

#### 遍历HBase并保存

![](assets/scanHBase.png)



#### Hive Shell表操作

平均次数大于300的词语：

![](assets/great300.png)

平均次数最多的100个词语：

![](assets/top100.png)

### 七、实验体会

参考：

【1】

【2】

【3】