import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.*;

public class WriteBoth2 {

    public static class WriteBoth2Reducer extends Reducer<WriteHBase.WordType, IntWritable, Text, Text> {
        //建立map来保存单词与平均出现次数的对应关系
        protected Map<String, Double> table = new HashMap<>();

        @Override
        public void reduce(WriteHBase.WordType key, Iterable<IntWritable> values, Context context)throws IOException,InterruptedException{
            double sum = 0;//出现次数和
            //建立书名和出现次数的映射关系
            Map<String, Integer> books = new HashMap<String, Integer>();
            //遍历value算出词频总和，建立书名和词频的映射关系
            for (IntWritable it : values) {
                int num = Integer.parseInt(it.toString());//得到出现次数
                sum += num;//次数和更新
                String bookname = key.GetFilename();//得到书名
                if (books.containsKey(bookname))//判断是否已经保存过这本书
                    //若保存过，更新出现次数
                    books.put(bookname, books.get(bookname) + num);
                else {
                    //未保存过，则向map中添加该映射关系
                    books.put(bookname, num);
                }
            }
            //得到平均出现次数
            double average = sum / (double) (books.size());
            StringBuilder result = new StringBuilder();//输出倒排索引
            //建立倒排索引
            for (String k : books.keySet()) {
                result.append(k).append(":").append(Integer.toString(books.get(k))).append("; ");
            }
            String word = key.GetWord();
            //向表格中添加<单词，平均出现次数>的映射关系
            table.put(word, average);
            //将倒排索引写入HDFS文件
            context.write(new Text(word), new Text(result.toString()));

        }
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Configuration configuration = HBaseConfiguration.create();//得到hBase资源的Configuration
            configuration.set("hbase.zookeeper.quorum", "localhost");//设定zookeeper属性
            HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
            HTable Htable = new HTable(configuration, WriteHBase.tablename);//根据表名得到对应的表
            List<Put> puts = new ArrayList<>();
            //遍历Map，对每个单词构造一个行加入list中
            for(Map.Entry<String, Double>entry : table.entrySet()){
                String word = entry.getKey();
                Double num = entry.getValue();
                //根据word的rowkey来实现put
                Put put = new Put(Bytes.toBytes(word));
                //根据列族名和列确定插入位置
                put.add(Bytes.toBytes(WriteHBase.colfamily), Bytes.toBytes(WriteHBase.col), Bytes.toBytes(num));
                puts.add(put);
            }
            //向表中添加
            Htable.put(puts);

            super.cleanup(context);
        }
    }

    public static void main(String[] args)throws Exception {
        //得到表名
        String tablename = WriteHBase.tablename;
        String colfamily = WriteHBase.colfamily;//得到列族名
        Configuration configuration = HBaseConfiguration.create();//得到hBase资源的Configuration
        configuration.set("hbase.zookeeper.quorum", "localhost");//设定zookeeper属性
        HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
        if (hBaseAdmin.tableExists(tablename)) {//检查是否已经存在该表
            System.out.println("table exists!recreating......");
            hBaseAdmin.disableTable(tablename);//将旧的表disable
            hBaseAdmin.deleteTable(tablename);//将旧的表删除
        }
        HTableDescriptor htd = new HTableDescriptor(tablename);//创建描述表名
        HColumnDescriptor hcd = new HColumnDescriptor(colfamily);
        htd.addFamily(hcd);//添加列族
        hBaseAdmin.createTable(htd);//创建表

        //开始初始化job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Write HBase and HDFS");
        //Job job = new Job(conf, "word count");

        job.setJarByClass(WriteBoth2.class);
        job.setMapperClass(WriteHBase.InvertIndexMapper.class);//设定Mapper类
        job.setReducerClass(WriteBoth2.WriteBoth2Reducer.class);//设定Reducer类

        job.setMapOutputKeyClass(WriteHBase.WordType.class);//设定Mapper输出key类型
        job.setMapOutputValueClass(IntWritable.class);//设定Mapper输出Value类型
        job.setOutputKeyClass(Text.class);//设定最终输出key类型
        job.setOutputValueClass(Text.class);//设定最终输出value类型

        FileInputFormat.addInputPath(job, new Path(args[0]));//添加输出文件的路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));//设定输出文件的路径

        TableMapReduceUtil.addDependencyJars(job);//添加依赖的jar包地址
        System.exit(job.waitForCompletion(true) ? 0 : 1);//等待退出
    }
}
