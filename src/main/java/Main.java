public class Main {
    public static void main(String[] args) throws Exception {
        System.out.println("Welcome to use Jin Yong Novel Analyse Program");
        System.out.println("Usage:");
        System.out.println("if Arguments == NULL then default run all tasks");
        System.out.println("if Arguments == task1 or task2 or task3 then default run task[1,2,3] only");
        System.out.println("if Arguments == task4 pre|final then run preprocessing or loop final only of task[4]");
        System.out.println("if Arguments == task4 loopi (i belongs to 0~9) then run iteration loop and start from loopi of task[4]");
        System.out.println("if Arguments == task5 tag then run RawTagCreator of task[5] only");
        System.out.println("if Arguments == task5 loopi (i belongs to 0~9) then run iteration loop and start from loopi of task[5]");
        if(args.length == 0) {
            ReadNovel.main(null);
            PageRank.main(null);
            RawTagCreator.main(null);
            LPA.main(null);
        }
        else if(args[0].equals("task1")){
            System.out.println("Running: Task1");
            ReadNovel.main(args);
        }
        else if(args[0].equals("task2")){
            System.out.println("Running: Task2");
            ReadNovel.main(args);
        }
        else if(args[0].equals("task3")){
            System.out.println("Running: Task3");
            ReadNovel.main(args);
        }
        else if(args[0].equals("task4") && args.length >= 2){
            if (args[1].equals("final")) {
                System.out.println("Running: Task4-Final");
                PageRank.main(new String[]{"final"});
            }
            else if (args[1].equals("pre")) {
                System.out.println("Running: Task4-Pre");
                PageRank.main(new String[]{"pre"});
            }
            else
                for(int i = 0; i < PageRank.loopTimes; ++i)
                    if(args[1].equals("loop" + i)) {
                        System.out.println("Running: Task4-Loop" + i);
                        PageRank.main(new String[]{String.valueOf(i)});
                    }
        }
        else if(args[0].equals("task5") && args.length >= 2){
            if (args[1].equals("tag")) {
                System.out.println("Running: Task5-Tag");
                RawTagCreator.main(args);
            }
            else
                for (int i = 0; i < LPA.max_times; ++i)
                    if (args[1].equals("loop" + i)) {
                        System.out.println("Running: Task5-Loop"+i);
                        LPA.main(new String[]{String.valueOf(i)});
                    }
        }
    }
}
