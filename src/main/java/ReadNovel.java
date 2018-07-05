import java.io.FileReader;
import java.util.List;
import java.io.BufferedReader;
import java.io.IOException;

import org.ansj.library.UserDefineLibrary;
import org.ansj.splitWord.analysis.DicAnalysis;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.ansj.domain.Term;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

@SuppressWarnings("deprecation")
public class ReadNovel {

    public static class ReadNovelMapper extends Mapper<Object,Text,Text,Text> {
        private org.apache.hadoop.fs.Path[] NameFilePath;
        @SuppressWarnings("resource")
        public void setup(Context context) throws IOException {
            NameFilePath = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            String line;
            BufferedReader br = new BufferedReader(new FileReader(NameFilePath[0].toString()));
            while ((line = br.readLine()) != null) {
                java.util.StringTokenizer sTokenizer = new java.util.StringTokenizer(line);
                UserDefineLibrary.insertWord(sTokenizer.nextToken(), "RoleName", 1000);
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit FileSplit = (FileSplit) context.getInputSplit();
            String filename = FileSplit.getPath().getName();
            List<Term> After = DicAnalysis.parse(value.toString()).getTerms();
            HashMap<String, Integer> NameMap = new HashMap<String, Integer>();
            for (Term x : After) {
                if (x.getNatureStr().equals(new String("RoleName"))) {
                    String rName = x.getName();
                    if ((rName.equals(new String("大汉")) && !filename.equals(new String("金庸01飞狐外传.txt")))) continue;
                    if ((rName.equals(new String("汉子")) && !filename.equals(new String("金庸11侠客行.txt")))) continue;
                    if ((rName.equals(new String("说不得")) && !filename.equals(new String("金庸12倚天屠龙记.txt")))) continue;
                    NameMap.put(rName, 1);
                }
            }
            Set<Entry<String, Integer>> set = NameMap.entrySet();
            Iterator<Entry<String, Integer>> it0 = set.iterator();
            while (it0.hasNext()) {
                Entry<String, Integer> aEntry = it0.next();
                Iterator<Entry<String, Integer>> it1 = set.iterator();
                while (it1.hasNext()) {
                    Entry<String, Integer> bEntry = it1.next();
                    if (!aEntry.equals(bEntry)) {
                        int av = aEntry.getValue();
                        int bv = bEntry.getValue();
                        if (av > bv) {
                            int t = av;
                            av = bv;
                            bv = t;
                        }
                        for (int i = 0; i < av; i++) {
                            context.write(new Text(aEntry.getKey()), new Text(bEntry.getKey()));
                        }
                    }
                }
            }
        }
    }

    public static class ReadNovelReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key,Iterable<Text> value,Context context) throws IOException, InterruptedException{
            HashMap<String, Integer> vMap=new HashMap<String,Integer>();
            for(Text x:value){
                String vString=x.toString();
                if(vMap.containsKey(vString))
                    vMap.put(vString, vMap.get(vString).intValue()+1);
                else vMap.put(vString, 1);
            }
            Set<Entry<String, Integer>> entry = vMap.entrySet();
            int aValue=0;
            Iterator<Entry<String, Integer>> it=entry.iterator();
            while(it.hasNext()){
                Entry<String,Integer> t=it.next();
                aValue += t.getValue();
            }
            it=entry.iterator();
            StringBuilder sb=new StringBuilder();

            Entry<String,Integer> t=it.next();
            String tName=t.getKey();
            double tValue=(t.getValue() * 1.0)/aValue;
            sb.append(tName+":"+tValue);
            while(it.hasNext()){
                t=it.next();
                tName=t.getKey();
                tValue=(t.getValue() * 1.0)/aValue;
                sb.append(","+tName+":"+tValue);
            }
            context.write(key,new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        try{
            Configuration conf = new Configuration();
            DistributedCache.addCacheFile(new Path("/data/task2/people_name_list.txt").toUri(),conf);
            Job job= new Job(conf,"Read Novel");
            job.setJarByClass(ReadNovel.class);
            job.setMapperClass(ReadNovelMapper.class);
            job.setReducerClass(ReadNovelReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path("/data/task2/novels"));
            FileOutputFormat.setOutputPath(job, new Path("/user/2018st04/output"));
            job.waitForCompletion(true);
        }
        catch (Exception ex){
            ex.printStackTrace();
        }
    }
}