# InvertedIndexWithHbase
Part of Nju big data Lab4: Program using Hbase jdbc  

## 功能及使用说明

#### 1.修改第3次实验的MapReduce程序，在Reduce阶段将倒排索引的信息通过文件输出， 而每个词语及其对应的“平均出现次数”信息写入到HBase的表“Wuxia”中。

使用指令

```
sbin/hadoop jar <jar name>.jar WriteBoth2 <input file path> <output file path>
```

其中jar name由pom决定，WriteHBase是对应功能main函数所在的类，input file path必须是集群中的地址

#### 2.编写Java程序，遍历上一步中保存在HBase中的表，并把表格的内容(词语以及平 均出现次数)保存到本地文件中。

使用指令

```
sbin/hadoop jar <jar name>.jar ReadHBase <table name> <output file path> 
```

ReadHBase是对应功能main函数所在的类，input file path是集群中输出的地址

## 代码简介

#### 1.WriteBoth2.java

完成上述功能1。使用自定义类WordType作为Map的key。定义Mapper类为InvertIndexMapper，完成按<单词,书名>的map，WordType的覆盖hashcode方法保证同个单词进入同个reducer中。

reducer中将平均出现次数写入Wuxia表中，表的rowKey为单词，列族（column family）为Content，对应平均次数对应列名为average num。（有关这些的参数都在WriteHBase中的static变量中访问得到），为了写HBase中的表，在Reducer中创建Map来储存<单词，平均出现次数>这个键值对。Reducer中完成对这个map的填充，在最后的cleanup中实现写入HBase中

#### 1.ReadHBase.java

完成功能2。使用MapReduce框架。Mapper类使用TableMapper，参数中ImmutableBytesWritable 类型的key,可通过key.get()得到Bytes类型的 rowkey，Result类型的value可通过getFamilyMap（Bytes(colname)）来得到一个<列名，对应储存值>的Map，从而可以完成键值对<单词，平均出现次数>传入Reducer。Reducer不用怎么处理就可以直接输出。



