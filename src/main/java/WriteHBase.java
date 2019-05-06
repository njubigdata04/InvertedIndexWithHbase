import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

public class WriteHBase {

    //自定义属性
    public static String colfamily = "Content";
    public static String col = "average num";
    public static String tablename = "Wuxia";

    //定义组合key
    public static class WordType implements WritableComparable<WordType> {
        private String word;
        private String filename;

        public WordType() {

        }

        public WordType(String w, String f) {
            word = w;
            filename = f;
        }

        public String GetWord() {
            return word;
        }

        public String GetFilename() {
            return filename;
        }

        public void SetWord(String w) {
            word = w;
        }

        public void SetFilename(String f) {
            filename = f;
        }

        //@Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(word);
            out.writeUTF(filename);
        }

        //@Override
        public void readFields(DataInput in) throws IOException {
            word = in.readUTF();
            filename = in.readUTF();
        }

        //@Override
        public int compareTo(WordType o) {
            //return word.compareTo(o.word);

            if (word == o.word)
                return filename.compareTo(o.filename);
            else
                return word.compareTo(o.word);

        }

        @Override
        public int hashCode() {
            return word.hashCode();
        }
    }


    /*public static class InvertIndexMapper extends Mapper<Object, Text, WordType, Text> {

        //private final static IntWritable one = new IntWritable(1);
        //private Text word = new Text();

        public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filename = fileSplit.getPath().getName();
            String[] str = filename.split("\\.");
            String book = "";
            for (int i = 0; i < str.length - 3; i++)
                book += str[i] + ".";
            book += str[str.length - 3];
            //filename = str[0];
            //String book = str[1];
            //str = filename.split("\\d");
            //String author = str[0];
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                WordType wordType = new WordType(itr.nextToken(), book);
                //word.set(itr.nextToken());
                context.write(wordType, new Text("1"));
            }
        }
    }*/
    public static class InvertIndexMapper extends Mapper<Object, Text, WordType, IntWritable> {

        //private final static IntWritable one = new IntWritable(1);
        //private Text word = new Text();

        public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filename = fileSplit.getPath().getName();
            String[] str = filename.split("\\.");
            String book = "";
            for (int i = 0; i < str.length - 3; i++)
                book += str[i] + ".";
            book += str[str.length - 3];
            //filename = str[0];
            //String book = str[1];
            //str = filename.split("\\d");
            //String author = str[0];
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                WordType wordType = new WordType(itr.nextToken(), book);
                //word.set(itr.nextToken());
                context.write(wordType, new IntWritable(1));
            }
        }
    }

    public static class InvertIndexCombiner extends Reducer<WordType, Text, WordType, Text> {
        public void reduce(WordType key, Iterable<Text> value, Context context) throws InterruptedException, IOException {
            int sum = 0;
            for (Text it : value) {
                sum += Integer.parseInt(it.toString());
            }
            context.write(key, new Text(Integer.toString(sum)));
        }
    }

    /*public static class WriteHBaseReducer extends TableReducer<WordType, Text, NullWritable>{
        public static NullWritable OUT_PUT_KEY = NullWritable.get();
        public Put outputValue;
        @Override
        protected void reduce(WordType key, Iterable<Text> values, Context context)throws IOException,InterruptedException{
            double sum = 0;
            Map<String, Integer> books = new HashMap<String, Integer>();
            for (Text it : values) {
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
            Put put = new Put(key.GetWord().getBytes());
            put.addColumn(Bytes.toBytes("Average"), Bytes.toBytes("average num"), Bytes.toBytes(average));
            context.write(OUT_PUT_KEY, put);
        }
    }*/
    public static class WriteHBaseReducer extends TableReducer<WordType, IntWritable, NullWritable>{
        public static NullWritable OUT_PUT_KEY = NullWritable.get();
        public Put outputValue;
        @Override
        protected void reduce(WordType key, Iterable<IntWritable> values, Context context)throws IOException,InterruptedException{
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
            /*String result = Double.toString(average) + ", ";
            Iterator<String> iterator = books.keySet().iterator();
            while (iterator.hasNext()) {
                String k = iterator.next();
                result = result + (k + ":" + Integer.toString(books.get(k)) + "; ");
            }*/
            Put put = new Put(key.GetWord().getBytes());
            put.addColumn(Bytes.toBytes(WriteHBase.colfamily), Bytes.toBytes(WriteHBase.col), Bytes.toBytes(average));
            context.write(OUT_PUT_KEY, put);
        }
    }

    public static void main(String[] args) throws Exception {
        String colfamily = WriteHBase.colfamily;
        String col = WriteHBase.col;
        String tablename = WriteHBase.tablename;
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
        FileInputFormat.addInputPath(job, new Path(args[0]));
        TableMapReduceUtil.initTableReducerJob(tablename, WriteHBase.WriteHBaseReducer.class, job);
        TableMapReduceUtil.addDependencyJars(job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
