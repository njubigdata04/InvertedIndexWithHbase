import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Map;

public class ReadHBase {
    public static class ReadHBaseMapper extends TableMapper<Text, Text>{
        protected void map(ImmutableBytesWritable key, Result value, Context context)throws IOException, InterruptedException{
            //StringBuffer sb = new StringBuffer("");
            //key是rowkey， value是一个map of qualifiers to values
            for(Map.Entry<byte[], byte[]> entry: value.getFamilyMap(WriteHBase.colfamily.getBytes()).entrySet()){
                String str = new String(Bytes.toString(entry.getKey()));//将rowkey转成String
                Double num = (Bytes.toDouble(entry.getValue()));//得到平均出现次数
                String sb = num.toString();//将平均出现次数转成Double类型
                //转成String
                /*if(str != null){
                    sb.append(new String(entry.getValue()));
                    //sb.append("\t");
                    //sb.append(str);
                }*/
                //输出（单词，平均出现次数）
                context.write(new Text(Bytes.toString(key.get())), new Text(sb));
            }
        }

    }
    public static class ReadHBaseReducer extends Reducer<Text, Text, Text, Text>{
        protected void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException{
            //key是单词，values是对应的平均出现次数
            for(Text val:values){
                //写入HDFS文件
                context.write(new Text(key.toString()), new Text(val.toString()));
            }
        }
    }
    public static void main(String[] args) throws Exception{
        String tablename = args[0];//得到表格名
        Configuration conf = HBaseConfiguration.create();//得到HBase对应的Configuration
        conf.set("hbase.zookeeper.quorum", "localhost");//设定zookeeper属性
        /*String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: WordCountHbaseReader <out>");
            System.exit(2);
        }*/
        String input = args[1];//输入路径
        Job job = new Job(conf, "ReadHBase");//建立Job
        job.setJarByClass(ReadHBase.class);//Set the Jar by finding where a given class came from.
        FileOutputFormat.setOutputPath(job, new Path(input));//设定输出路径
        job.setReducerClass(ReadHBaseReducer.class);//设定Reducer的类
        Scan scan = new Scan();//创建Scan
        //初始化Mapper，与HBase联系起来
        TableMapReduceUtil.initTableMapperJob(tablename, scan, ReadHBaseMapper.class, Text.class, Text.class, job);
        System.exit(job.waitForCompletion(true)? 0:1);
    }
}
