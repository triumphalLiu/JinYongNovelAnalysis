import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class PageRank {
    /*PageRank排序类*/
    public static class PageRankViewer {
        public static void pageRankViewer(String inputPath, String outputPath) throws InterruptedException, IOException, ClassNotFoundException {
            Configuration conf=new Configuration();
            Job job = new Job(conf,"Part4.3 - PageRank Sort");
            job.setJarByClass(PageRankViewer.class);
            job.setMapperClass(PageRankViewerMapper.class);
            job.setMapOutputKeyClass(DoubleWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setSortComparatorClass(DoubleWritableDecressingComparator.class);
            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            job.waitForCompletion(true);
        }

        public static class PageRankViewerMapper extends Mapper<Object,Text,DoubleWritable,Text>
        {

            public void map(Object key,Text value,Context context) throws IOException,InterruptedException {
                String[] line = value.toString().split("\t");
                String page = line[0];
                double pr = Double.parseDouble(line[1]);
                context.write(new DoubleWritable(pr),new Text(page));
            }
        }

        private static class DoubleWritableDecressingComparator extends DoubleWritable.Comparator {
            public int compare(WritableComparable a,WritableComparable b) {
                return -super.compare(a,b);
            }
            /*二进制数据的字典顺序*/
            public int compare(byte[] b1,int s1,int l1,byte[] b2,int s2,int l2) {
                return -super.compare(b1,s1,l1,b2,s2,l2);
            }
        }
    }

    /*PageRank值迭代计算类*/
    public static class PageRankIter{
        public static void pageRankIter(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
            Configuration conf=new Configuration();
            Job job = new Job(conf,"Part4.2 - PageRank Calculate");
            job.setJarByClass(PageRankIter.class);
            job.setMapperClass(PRIterMapper.class);
            job.setReducerClass(PRIterReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            job.waitForCompletion(true);
        }

        public static class PRIterMapper extends Mapper<LongWritable,Text,Text,Text> {
            public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException {
                String line =value.toString();
                String[] tokens = line.split("\t");
                String pageKey=tokens[0];
                double pr_init = Double.parseDouble(tokens[1]);
                String links;
                if(tokens.length>=3) {
                    links = tokens[2];
                    /*name1:a|name2:b|name3:c*/
                    /*   |  具体特殊含义！！！！得转义*/
                    String[] linkPages = links.split("\\|");
                    for (String stri : linkPages) {
                         /*name1:a*/
                        String[] tmp = stri.split(":");
                        if(tmp.length < 2)
                            continue;
                        double proportion = Double.parseDouble(tmp[1]);
                        String pr_value = pageKey + "\t" + String.valueOf(pr_init * proportion);
                        context.write(new Text(tmp[0]), new Text(pr_value));
                    }
                    /*在迭代过程中，必须保留原来的链出信息，以维护图的结构添加 "|" 供区分*/
                    context.write(new Text(pageKey), new Text("," + links));
                }
            }
        }

        public static class PRIterReducer extends Reducer<Text,Text,Text,Text>
        {
            public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
                String links = "";
                double pr = 0;
                for( Text value:values)
                {
                    String tmp = value.toString();
                    if(tmp.charAt(0)==','){

                        links  = "\t" ;
                        for(int j=1;j<tmp.length();j++)
                            links+=tmp.charAt(j);
                        continue;
                    }
                    pr += Double.parseDouble(tmp.split("\t")[1]);
                }
                context.write(new Text(key),new Text(String.valueOf(pr) + links ));
            }
        }
    }
    /*创建邻接图类*/
    public static class CreateGraph {
        public static class CreateGraphMapper extends Mapper<Object, Text, Text, Text> {
            public void map(Object key, Text value, Context context)
                    throws IOException, InterruptedException {
                String pagerank = "1.0\t";
                String str=value.toString();
                Text name = new Text(str.split("\t")[0]);
                pagerank += str.split("\t")[1];
                /*生成 name   1.0 name1:a|name2:b|name3:c*/
                context.write(name, new Text(pagerank));
            }
        }

        public static void createGraph(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
            Configuration conf = new Configuration();
            Job job = new Job(conf, "Task4.1 - Create Graph");
            job.setJarByClass(CreateGraph.class);
            job.setMapperClass(CreateGraphMapper.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            job.waitForCompletion(true);
        }
    }

    /*进行pagerank操作的入口*/
    private static final int loopTimes = 10;
    public static void main(String[] args)throws Exception {
        CreateGraph.createGraph("/user/2018st04/ReadNovelOutput", "/user/2018st04/PRData0");
        for(int i = 0; i < loopTimes; i++)
            PageRankIter.pageRankIter("/user/2018st04/PRData" + i, "/user/2018st04/PRData" + String.valueOf(i + 1));
        PageRankViewer.pageRankViewer("/user/2018st04/PRData" + loopTimes, "/user/2018st04/FinalRank");
    }
}
