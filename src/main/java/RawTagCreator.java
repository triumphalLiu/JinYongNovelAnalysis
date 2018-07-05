import java.io.*;

public class RawTagCreator {

    public static void createEmptyRawTag() throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("/data/task2/people_name_list.txt"), "UTF-8"));
        String line = null;
        FileWriter fileWriter = new FileWriter(new File("./RawTag.txt"));
        while ((line = br.readLine()) != null) {
            if(line.equals("")) break;
            int value = 0;
            if     (line.equals("胡斐")   || line.equals("程灵素")   || line.equals("袁紫衣")) value = 1;
            else if(line.equals("胡一刀") || line.equals("苗人凤")   || line.equals("田归农")) value = 2;
            else if(line.equals("狄云")   || line.equals("丁典")     || line.equals("戚长发")) value = 3;
            else if(line.equals("乔峰")   || line.equals("段誉")     || line.equals("阿朱"))   value = 4;
            else if(line.equals("郭靖")   || line.equals("黄蓉")     || line.equals("洪七公")) value = 5;
            else if(line.equals("李文秀") || line.equals("华辉")     || line.equals("阿曼"))   value = 6;
            else if(line.equals("韦小宝") || line.equals("鳌拜")     || line.equals("阿双"))   value = 7;
            else if(line.equals("令狐冲") || line.equals("岳灵珊")   || line.equals("向问天")) value = 8;
            else if(line.equals("陈家洛") || line.equals("香香公主") || line.equals("霍青桐")) value = 9;
            else if(line.equals("杨过")   || line.equals("小龙女")   || line.equals("郭襄"))   value = 10;
            else if(line.equals("石破天") || line.equals("丁不三")   || line.equals("谢烟客")) value = 11;
            else if(line.equals("张无忌") || line.equals("杨逍")     || line.equals("张三丰")) value = 12;
            else if(line.equals("袁承志") || line.equals("金蛇郎君") || line.equals("夏青青")) value = 13;
            else if(line.equals("袁冠南") || line.equals("林玉龙")   || line.equals("任飞燕")) value = 14;
            fileWriter.write(line + " " + value + "\n");
        }
        fileWriter.flush();
        fileWriter.close();
        br.close();
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        File rawTagPath = new File("./RawTag.txt");
        if(rawTagPath.exists() == false || rawTagPath.isFile() == false) {
            rawTagPath.createNewFile();
            createEmptyRawTag();
        }
    }
}