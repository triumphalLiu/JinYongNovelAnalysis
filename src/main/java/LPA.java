import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.Map.Entry;

public class LPA {
    private static int max_times = 10;
    private static HashMap<String,Integer> label_map = new HashMap<String, Integer>();
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        for(int times = 0 ; times<max_times ; times++ ){
            Configuration conf=new Configuration();
            conf.set("fs.hdfs.impl.disable.cache", "true");
            //HashMap<String, Integer> older = new HashMap<String,Integer>();
            FileSystem hdfs=FileSystem.get(conf);
            Scanner temp_sc = new Scanner(hdfs.open(new Path("./RawTag.txt")),"UTF-8");
            while(temp_sc.hasNextLine()){
                StringTokenizer temp_st = new StringTokenizer(temp_sc.nextLine());
                String temp_key = temp_st.nextToken();
                String temp_value = temp_st.nextToken();
                label_map.put(temp_key , Integer.parseInt(temp_value));
            }
            temp_sc.close();

            Job job=new Job(conf,"Task5_LPA");
            job.setJarByClass(LPA.class);
            job.setMapperClass(LPAMapper.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path("./ReadNovelOutput"));
            FileOutputFormat.setOutputPath(job, new Path("./RawTag" + times));
            job.waitForCompletion(true);
        }
    }
    public static class LPAMapper extends Mapper<Object, Text, Text, Text> {
        HashMap<String,Integer> temp_label1 = new HashMap<String, Integer>();
        HashMap<String,Integer> temp_label2 = new HashMap<String, Integer>();
        FileSystem hdfs;

        //用于初始化键值对列表
        public void setup(Context context) throws IOException{
            hdfs=FileSystem.get(context.getConfiguration());
            Scanner open_file = new Scanner(hdfs.open(new Path("./RawTag.txt")),"UTF-8");
            while(open_file.hasNextLine()){
                StringTokenizer st0 = new StringTokenizer(open_file.nextLine());
                String word = st0.nextToken();
                String tag = st0.nextToken();
                temp_label1.put(word, Integer.parseInt(tag));
            }
            open_file.close();
        }

        //用于map集群计算
        public void map(Object my_key, Text my_value, Context context){
            StringTokenizer st1 = new StringTokenizer(my_value.toString());
            //st1 表示关系列表中某一项所有关系的内容
            String nextWord = st1.nextToken();
            if(!temp_label1.containsKey(nextWord))
                return ;
            //nextWord 表示第一个key的值
            StringTokenizer st2 = new StringTokenizer(st1.nextToken(),"|");
            // st2 表示某一个人名与前面人名的关系记录
            double [] my_list = new double [15];
            for(int i = 0; i < 15; i++ )
                my_list[i] = 0;
            //int text_num = 0;
            while(st2.hasMoreTokens()){
                StringTokenizer st3 = new StringTokenizer(st2.nextToken(),":");
                String st_name = st3.nextToken();
                //st_name 表示关系列表中某一项的人名
                double st_value = Double.parseDouble(st3.nextToken());
                //st_name 表示关系列表中当前人名的权重
                if(!temp_label1.containsKey(st_name))
                    continue;
                //查找当前人名是否存在于人名类别列表中
                int temp_value = temp_label1.get(st_name);
                //找到当前人名的类别
                my_list[temp_value] += st_value;
                //text_num++;
            }
            int temp_max = 1;
            for(int i=2;i<15;i++){
                if(my_list[i]>my_list[temp_max])
                    temp_max = i;
            }
            if(my_list[temp_max]>0)
                temp_label2.put(nextWord,temp_max);
        }

        //用于最后的数据写入
        public void cleanup(Context context) throws IOException{
            for(int times = 0 ; times<max_times ; times++ ){
                hdfs.delete(new Path("./RawTag" + times), true);
            }
            FSDataOutputStream out_put = hdfs.create(new Path("./RawTagFinal"));
            PrintWriter pr1 = new PrintWriter(out_put);
            Set<Entry<String,Integer>>set = temp_label2.entrySet();
            Iterator<Entry<String,Integer>> iterator = set.iterator();
            while(iterator.hasNext()){
                Entry<String,Integer> entry = iterator.next();
                pr1.println(new String(entry.getKey()+" "+entry.getValue()));
            }
            pr1.close();
            out_put.close();
        }

    }
}