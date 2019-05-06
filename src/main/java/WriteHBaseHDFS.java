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
        private MultipleOutputs multipleOutputs;
        //String outputPath;
        @SuppressWarnings("unchecked")
        protected void setup(Context context)throws IOException, InterruptedException{
            multipleOutputs = new MultipleOutputs(context);
            //outputPath = context.getConfiguration().get("outputPath");
        }
        @Override
        protected void reduce(WriteHBase.WordType key, Iterable<IntWritable> values, Context context)throws IOException,InterruptedException{
            double sum = 0;
            Map<String, Integer> books = new HashMap<String, Integer>();
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
            double average = sum / (double) (books.size());
            String result = "";
            Iterator<String> iterator = books.keySet().iterator();
            while (iterator.hasNext()) {
                String k = iterator.next();
                result = result + (k + ":" + Integer.toString(books.get(k)) + "; ");
            }
            Put put = new Put(key.GetWord().getBytes());
            put.addColumn(Bytes.toBytes(WriteHBase.colfamily), Bytes.toBytes(WriteHBase.col), Bytes.toBytes(average));
            context.write(OUT_PUT_KEY, put);
            multipleOutputs.write("hdfs", new Text(key.GetWord().toString()), new Text(result));
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

    static void main(String[] args)throws Exception {
        String tablename = WriteHBase.tablename;
        String colfamily = WriteHBase.colfamily;
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "localhost");
        HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
        if (hBaseAdmin.tableExists(tablename)) {
            System.out.println("table exists!recreating......");
            hBaseAdmin.disableTable(tablename);
            hBaseAdmin.deleteTable(tablename);
        }
        HTableDescriptor htd = new HTableDescriptor(tablename);
        HColumnDescriptor hcd = new HColumnDescriptor(colfamily);
        htd.addFamily(hcd);
        hBaseAdmin.createTable(htd);
        Job job = Job.getInstance(configuration);
        job.setJarByClass(WriteHBase.class);
        //job.setJar("BigData-1.0-SNAPSHOT.jar");
        job.setMapperClass(WriteHBase.InvertIndexMapper.class);
        //job.setCombinerClass(InvertIndexCombiner.class);
        job.setMapOutputKeyClass(WriteHBase.WordType.class);
        job.setMapOutputValueClass(IntWritable.class);
        //输入路径
        FileInputFormat.addInputPath(job, new Path(args[0]));

        //输出路径
        String outputPath = args[1];
        //job.getConfiguration().set("outputPath", outputPath);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        MultipleOutputs.addNamedOutput(job, "hdfs", TextOutputFormat.class, Text.class, Text.class);
        //MultipleOutputs.addNamedOutput(job, "hbase", TableOutputFormat.class, Text.class, Text.class);
        TableMapReduceUtil.initTableReducerJob(tablename, WriteHBaseHDFS.WriteHBaseHDFSReducer.class, job);
        TableMapReduceUtil.addDependencyJars(job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
