import javafx.scene.text.Text;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class WriteHBaseHDFS {
    public static class WriteHBaseHDFSReducer extends TableReducer<WriteHBase.WordType, IntWritable, NullWritable> {
        public static NullWritable OUT_PUT_KEY = NullWritable.get();
        //输出文件的功能类
        private MultipleOutputs multipleOutputs;
        String outputPath;
        @SuppressWarnings("unchecked")
        protected void setup(Context context)throws IOException, InterruptedException{
            multipleOutputs = new MultipleOutputs(context);//初始化
            outputPath = context.getConfiguration().get("outputPath");
        }
        @Override
        protected void reduce(WriteHBase.WordType key, Iterable<IntWritable> values, Context context)throws IOException,InterruptedException{
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
            //建立向表中添加的put对象
            Put put = new Put(key.GetWord().getBytes());
            //添加一列
            put.addColumn(Bytes.toBytes(WriteHBase.colfamily), Bytes.toBytes(WriteHBase.col), Bytes.toBytes(average));
            //输出Hdfs文件，记录的是
            multipleOutputs.write("hdfs", new Text(key.GetWord().toString()), new Text(result), outputPath);
            System.out.println("Writing to HDFS...");
            context.write(OUT_PUT_KEY, put);

        }
        protected void cleanup(Context context){
            try {
                multipleOutputs.close();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args)throws Exception {
        //得到表名
        String tablename = WriteHBase.tablename;
        String colfamily = WriteHBase.colfamily;//得到列族明
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
        Job job = Job.getInstance(configuration);//创建任务
        job.setJarByClass(WriteHBase.class);//设定任务属性
        //job.setJar("BigData-1.0-SNAPSHOT.jar");
        job.setMapperClass(WriteHBase.InvertIndexMapper.class);//设定执行的Mapper类
        //job.setCombinerClass(InvertIndexCombiner.class);
        job.setMapOutputKeyClass(WriteHBase.WordType.class);//设定Mapper输出key类型
        job.setMapOutputValueClass(IntWritable.class);//设定Mapper输出value
        //输入路径
        FileInputFormat.addInputPath(job, new Path(args[0]));//设定读入文件的路径

        //输出路径
        String outputPath = args[1];
        job.getConfiguration().set("outputPath", outputPath);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));//设定文件输出路径
        //向MultipleOutput中建立需要输出的文件名及输出类型
        MultipleOutputs.addNamedOutput(job, "hdfs", TextOutputFormat.class, Text.class, Text.class);
        //MultipleOutputs.addNamedOutput(job, "hbase", TableOutputFormat.class, Text.class, Text.class);
        //建立Reducer操作
        TableMapReduceUtil.initTableReducerJob(tablename, WriteHBaseHDFS.WriteHBaseHDFSReducer.class, job);
        TableMapReduceUtil.addDependencyJars(job);//添加依赖的jar包地址
        System.exit(job.waitForCompletion(true) ? 0 : 1);//等待退出
    }
}
