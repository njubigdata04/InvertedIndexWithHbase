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

        protected Map<String, Double> table;
        protected void setup(){
            table = new HashMap<>();
        }
        @Override
        public void reduce(WriteHBase.WordType key, Iterable<IntWritable> values, Context context)throws IOException,InterruptedException{
            double sum = 0;
            Map<String, Integer> books = new HashMap<String, Integer>();
            //遍历value算出词频总和，建立书名和词频的映射关系
            for (IntWritable it : values) {
                int num = Integer.parseInt(it.toString());
                sum += num;
                String bookname = key.GetFilename();
                if (books.containsKey(bookname))
                    books.put(bookname, books.get(bookname) + num);
                else {
                    books.put(bookname, num);
                }
            }
            //得到平均出现次数
            double average = sum / (double) (books.size());
            String result = "";//输出倒排索引
            Iterator<String> iterator = books.keySet().iterator();
            //建立倒排索引
            while (iterator.hasNext()) {
                String k = iterator.next();
                result = result + (k + ":" + Integer.toString(books.get(k)) + "; ");
            }
            String word = key.GetWord();

            table.put(word,new Double(average));

            context.write(new Text(word), new Text(result));

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
        job.setMapperClass(WriteHBase.InvertIndexMapper.class);
        job.setReducerClass(WriteBoth2.WriteBoth2Reducer.class);

        job.setMapOutputKeyClass(WriteHBase.WordType.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        TableMapReduceUtil.addDependencyJars(job);//添加依赖的jar包地址
        System.exit(job.waitForCompletion(true) ? 0 : 1);//等待退出
    }
}
