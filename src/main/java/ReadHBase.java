import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
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
            StringBuffer sb = new StringBuffer("");
            for(Map.Entry<byte[], byte[]> entry: value.getFamilyMap(WriteHBase.colfamily.getBytes()).entrySet()){
                String str = new String(entry.getValue());
                //转成String
                if(str != null){
                    sb.append(new String(entry.getValue()));
                    //sb.append("\t");
                    //sb.append(str);
                }
                context.write(new Text(key.get()), new Text(new String(sb)));
            }
        }

    }
    public static class ReadHBaseReducer extends Reducer<Text, Text, Text, Text>{
        protected void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException{
            for(Text val:values){
                context.write(key, new Text(val.toString()));
            }
        }
    }
    public static void main(String[] args) throws Exception{
        String tablename = args[0];
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: WordCountHbaseReader <out>");
            System.exit(2);
        }
        String input = args[1];
        Job job = new Job(conf, "ReadHBase");
        job.setJarByClass(ReadHBase.class);
        FileOutputFormat.setOutputPath(job, new Path(input));
        job.setReducerClass(ReadHBaseReducer.class);
        Scan scan = new Scan();
        TableMapReduceUtil.initTableMapperJob(tablename, scan, ReadHBaseMapper.class, Text.class, Text.class, job);
        System.exit(job.waitForCompletion(true)? 0:1);
    }
}
