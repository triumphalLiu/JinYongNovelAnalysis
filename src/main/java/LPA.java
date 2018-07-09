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
    public static int max_times = 3;
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        int times = 0;
        if(args !=  null && args.length > 0)
            times = Integer.valueOf(args[0]);
        for( ; times<max_times ; times++ ){
            Configuration conf=new Configuration();
			//新建一个配置文件储存信息
            conf.set("fs.hdfs.impl.disable.cache", "true");
			//这是为了将setup中创建的FileSystem实例储存在缓存中，
			//使得每个节点都能访问同一个实例而不会因为某个节点关闭连接出现异常
            Job job=new Job(conf,"Task5_LPA");
            job.setJarByClass(LPA.class);
            job.setMapperClass(LPAMapper.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path("./ReadNovelOutput"));
            FileOutputFormat.setOutputPath(job, new Path("./RawTag" + times));
			//因为要求输出目录之前不能存在，为了不与之前的输出发生冲突而选择每次输出不同的目录
			//也可以在结束后删除原目录解决该问题
            job.waitForCompletion(true);
        }
    }
    public static class LPAMapper extends Mapper<Object, Text, Text, Text> {
		//前两个object ,text 参数表示输入,后两个text,text 表示输出 .
        HashMap<String,Integer> temp_label1 = new HashMap<String, Integer>();
		//temp_label1用于从文件中读取数据
        HashMap<String,Integer> temp_label2 = new HashMap<String, Integer>();
		//temp_label2用于储存迭代后的数据并读入文件中
        FileSystem hdfs;

        //用于初始化键值对列表
        public void setup(Context context) throws IOException{
			//context是map任务运行中的一个上下文，包含了整个任务的全部信息，hdfs用于获取这些任务信息
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
			//my_key 表示输入的key和value，context用于写入处理后的数据
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
            else
                temp_label2.put(nextWord, 0);
        }

        //用于最后的数据写入
        public void cleanup(Context context) throws IOException{
            hdfs.delete(new Path("./RawTag.txt"));
			//先清除原来文件中的信息
            FSDataOutputStream out_put = hdfs.create(new Path("./RawTag.txt"));
            PrintWriter pr1 = new PrintWriter(out_put);
            Set<Entry<String,Integer>>set = temp_label2.entrySet();
			//设置映射项，里面有getkey(),getValue()方法
            Iterator<Entry<String,Integer>> iterator = set.iterator();
			//迭代器
            while(iterator.hasNext()){
                Entry<String,Integer> entry = iterator.next();
                pr1.println(new String(entry.getKey()+" "+entry.getValue()));
            }
            pr1.close();
            out_put.close();
        }

    }
}