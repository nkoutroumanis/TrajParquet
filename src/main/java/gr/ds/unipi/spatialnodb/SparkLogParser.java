package gr.ds.unipi.spatialnodb;

import scala.Tuple3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SparkLogParser {

    public static void main(String[] args) throws IOException {
        System.out.println(getTimeFromStates("/Users/nicholaskoutroumanis/Desktop/application_1781548784779_0035").length());
    }

    public static String getTimeFromStates(String filePath){
        try {
            BufferedReader br = new BufferedReader(new FileReader(filePath));
            String line;
            List<Long> times = new ArrayList<>();
            long submissionTime = 0;
            long completedTime = 0;

            while((line = br.readLine())!=null){

                if(line.contains("\"SparkListenerStageCompleted\"")){
                    int index = line.indexOf("\"Submission Time\":");
                    String line1 = line.substring(index);
                    submissionTime = Long.parseLong(line1.substring(0,line1.indexOf(",")).substring(line1.indexOf(":")+1));

                    index = line.indexOf("\"Completion Time\":");
                    line1 = line.substring(index);
                    completedTime = Long.parseLong(line1.substring(0,line1.indexOf(",")).substring(line1.indexOf(":")+1));

                    times.add(Math.round((completedTime-submissionTime)/1000.0));
                }
            }
            String result = times.stream()
                    .map(String::valueOf) // Convert each Long to String
                    .collect(Collectors.joining(","));

            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getProperty(String filePath, String property){
        try {
            BufferedReader br = new BufferedReader(new FileReader(filePath));
            String line;
            long number = 0;
            while((line = br.readLine())!=null){
                if(line.contains(property)){
                    int index = line.indexOf(property);
                    line = line.substring(index);
                    number = number + Long.parseLong(line.substring(0,line.indexOf(",")).substring(line.indexOf(":")+1));
                }
            }
            return String.valueOf(number/(1024*1024));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getPropertyMax(String filePath, String property){
        try {
            BufferedReader br = new BufferedReader(new FileReader(filePath));
            String line;
            long number = 0;
            while((line = br.readLine())!=null){
                if(line.contains(property)){
                    int index = line.indexOf(property);
                    line = line.substring(index);
                    long currentNumber = Long.parseLong(line.substring(0,line.indexOf(",")).substring(line.indexOf(":")+1));
                    if(number<currentNumber){
                        number = currentNumber;
                    }
                }
            }
            return String.valueOf(number/(1024*1024));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getPropertySumFromMaxesTasks(String filePath, String property){
        try {
            BufferedReader br = new BufferedReader(new FileReader(filePath));
            String line;
            HashMap<String, Long> hashMap = new HashMap<>();
            while((line = br.readLine())!=null){
                if(line.contains(property)){

                    int stageProperty = line.indexOf("Stage ID");
                    String stageLineProperty = line.substring(stageProperty);
                    long stageNum = Long.parseLong(stageLineProperty.substring(0,stageLineProperty.indexOf(",")).substring(stageLineProperty.indexOf(":")+1));

                    int taskProperty = line.indexOf("Task ID");
                    String taskLineProperty = line.substring(taskProperty);
                    long taskNum = Long.parseLong(taskLineProperty.substring(0,taskLineProperty.indexOf(",")).substring(taskLineProperty.indexOf(":")+1));

                    int indexProperty = line.indexOf(property);
                    String lineProperty = line.substring(indexProperty);
                    long currentNumber = Long.parseLong(lineProperty.substring(0,lineProperty.indexOf(",")).substring(lineProperty.indexOf(":")+1));

                    if(hashMap.containsKey(stageNum+":"+taskNum)){
                        long number = hashMap.get(stageNum+":"+taskNum);
                        if(number<currentNumber){
                            hashMap.replace(stageNum+":"+taskNum,currentNumber);
                        }
                    }else{
                        hashMap.put(stageNum+":"+taskNum, currentNumber);
                    }
                }
            }

            long sumValues = 0;
            for (Long value : hashMap.values()) {
                sumValues += value;
            }

            return String.valueOf(sumValues/(1024*1024));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public static Tuple3<String, String, String> fileLogAnalyzer(){
        Path directory = Paths.get(System.getProperty("user.home"));
        String pattern = "application_";
        String sparkLogValues ="";
        String sparkLogMb = "";
        String sparkLogMaxPeak = "";

        try (Stream<Path> files = Files.list(directory)) {
            List<Path> paths = files.filter(path -> path.getFileName().toString().startsWith(pattern)).collect(Collectors.toList());
            if(paths.isEmpty()){
                throw new Exception("The are no files starting with 'application_' in the path");
            }else if(paths.size()>1){
                throw new Exception("The are other files starting with 'application_' in the path");
            }
            sparkLogValues = SparkLogParser.getTimeFromStates(paths.get(0).toAbsolutePath().toString());
            sparkLogMb = SparkLogParser.getProperty(paths.get(0).toAbsolutePath().toString(), "\"Remote Bytes Read\"");
            sparkLogMaxPeak = SparkLogParser.getPropertyMax(paths.get(0).toAbsolutePath().toString(),"\"Peak Execution Memory\"");

            paths.get(0).toFile().delete();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return Tuple3.apply(sparkLogValues, sparkLogMb, sparkLogMaxPeak);
    }

    public static Tuple3<String, String, String> fileLogAnalyzer2(){
        Path directory = Paths.get(System.getProperty("user.home"));
        String pattern = "application_";
        String sparkLogValues ="";
        String sparkLogMb = "";
        String sparkLogTotalMaxPeak = "";

        try (Stream<Path> files = Files.list(directory)) {
            List<Path> paths = files.filter(path -> path.getFileName().toString().startsWith(pattern)).collect(Collectors.toList());
            if(paths.isEmpty()){
                throw new Exception("The are no files starting with 'application_' in the path");
            }else if(paths.size()>1){
                throw new Exception("The are other files starting with 'application_' in the path");
            }
            sparkLogValues = SparkLogParser.getTimeFromStates(paths.get(0).toAbsolutePath().toString());
            sparkLogMb = SparkLogParser.getProperty(paths.get(0).toAbsolutePath().toString(), "\"Remote Bytes Read\"");
            sparkLogTotalMaxPeak = SparkLogParser.getPropertySumFromMaxesTasks(paths.get(0).toAbsolutePath().toString(),"\"Peak Execution Memory\"");

            paths.get(0).toFile().delete();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return Tuple3.apply(sparkLogValues, sparkLogMb, sparkLogTotalMaxPeak);
    }

}
