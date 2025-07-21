import javax.xml.parsers.*;
import org.w3c.dom.*;
import java.io.File;
import java.io.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class my_workflow {
    static class Job {
    String id;
    double runtime;
    double mi;
     double subDeadline;
     double slack;
    List<String> children = new ArrayList<>();
    List<String> parents = new ArrayList<>();
     double startTime;
    double endTime;
    VMData assignedVM;
    List<String>inputFiles;
    List<String>outputFiles;
    public Job(String id, double runtime, double mi,List<String>inputFiles,List<String>outputFiles) {
        this.id = id;
        this.runtime = runtime;
        this.mi=mi;
        subDeadline=0.0;
        slack=0.0;
        this.inputFiles=inputFiles;
        this.outputFiles=outputFiles;
    }

    @Override
    public String toString() {
        return "Job{id='" + id + "', mi=" + mi + ", parents=" + parents + ", children=" + children + "}";
    }
}

static class ScheduledTask {
    String jobId;
    String vmId;
    double startTime;
    double endTime;

    public ScheduledTask(String jobId, String vmId, double startTime, double endTime) {
        this.jobId = jobId;
        this.vmId = vmId;
        this.startTime = startTime;
        this.endTime = endTime;
    }

}

static class ScheduledJob {
    String jobId;
    
    double startTime;
    double endTime;

    public ScheduledJob(String jobId,  double startTime, double endTime) {
        this.jobId = jobId;
        
        this.startTime = startTime;
        this.endTime = endTime;
    }

}

static class ScheduleResult {
    public List<Job> schedule;
    public double totalCost;

    public ScheduleResult(List<Job> schedule, double totalCost) {
        this.schedule = schedule;
        this.totalCost = totalCost;
    }
}


    public static void main(String[] args) {

        Scanner sc=new Scanner(System.in);
        // System.out.println("Enter deadline(in miliseconds):");
        // int userDeadline=sc.nextInt();
        try {
            List<Double> deadlineList = generateDeadlines(8000, 100000, 1000);
            List<Double> costs = new ArrayList<>();
            List<String> files=Arrays.asList("CyberShake_30.xml", "Epigenomics_46.xml", "Montage_50.xml", "Inspiral_30.xml",
            "Sipht_30.xml");
            double dfactors[]={1.0,1.1,1,2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 2.0};
            List<String[]> rows = new ArrayList<>();

            for(String file:files)
           { for(double df:dfactors)
           {
            File xmlFile = new File(file); // Replace with your file
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(xmlFile);
   
            doc.getDocumentElement().normalize();

            Map<String, Job> taskMap = new HashMap<>();
            Map<String,String>fileToProducerTaskMap=new HashMap<>();
            Map<String,Double>fileSizeMap=new HashMap<>();
            List<VMData>vms=VMData.parseCSV("cleaned_vm_data.csv"); 
            double median_mips=getMedianMIPS(vms); //finding the mean MIPS to estimate the MI for each task in the taskMap
            double median_bw=getMedianBW(vms);
            // Extract all jobs
            NodeList jobList = doc.getElementsByTagNameNS("*", "job");
            Set<String> jobIds = new HashSet<>();
            for (int i = 0; i < jobList.getLength(); i++) {
                Element job = (Element) jobList.item(i);
                jobIds.add(job.getAttribute("id"));
                String jobId = job.getAttribute("id");
                 double runtime = Double.parseDouble(job.getAttribute("runtime"));
                 double mi=0;
                 mi=runtime*median_mips;
    
                //Now getting the input and output file lists
                NodeList usesList = job.getElementsByTagName("uses");
                List<String>inputFiles=new ArrayList<>();
                List<String>outputFiles=new ArrayList<>();
                  for (int j = 0; j < usesList.getLength(); j++) {
        Element usesElement = (Element) usesList.item(j);

        String fileName = usesElement.getAttribute("file");
        String linkType = usesElement.getAttribute("link"); 
        String sizeStr = usesElement.getAttribute("size");
        double size = sizeStr.isEmpty() ? 0.0 : Double.parseDouble(sizeStr);

        // For output → build file → producer mapping
        if (linkType.equalsIgnoreCase("output")) {
            fileToProducerTaskMap.put(fileName, jobId);
            fileSizeMap.put(fileName, size);
            outputFiles.add(fileName);
        }

        // For input → add to task’s inputFiles
        else if (linkType.equalsIgnoreCase("input")) {
          inputFiles.add(fileName);
        }
    }

                Job newJob = new Job(jobId, runtime,mi,inputFiles,outputFiles);
                taskMap.put(jobId, newJob);
            }

            // Extract dependencies
            NodeList childList = doc.getElementsByTagNameNS("*", "child");
            for (int i = 0; i < childList.getLength(); i++) {
                Element child = (Element) childList.item(i);
                String childId = child.getAttribute("ref");

                NodeList parents = child.getElementsByTagNameNS("*", "parent");
                for (int j = 0; j < parents.getLength(); j++) {
                    Element parent = (Element) parents.item(j);
                    String parentId = parent.getAttribute("ref");
                    taskMap.get(parentId).children.add(childId);
                    taskMap.get(childId).parents.add(parentId);
                }
            }

            List<String>sortedJobs = topologicalSort(taskMap);

           Map<String,Double>UpwardRanks= calculateUpwardRanks(sortedJobs, taskMap);
            double entryRank=0.0;
           for(Map.Entry<String,Double>entry:UpwardRanks.entrySet()){
            double rank=entry.getValue();
            entryRank=Math.max(rank,entryRank);
           
           }
           double userDeadline=df*entryRank;
           
           for( Map.Entry<String,Job>entry:taskMap.entrySet()){
            Job current=entry.getValue();
            current.subDeadline = (UpwardRanks.get(current.id) / entryRank) * userDeadline;
            //current.slack = current.subDeadline - UpwardRanks.get(current.id);
           }
             double slackThreshold = userDeadline * 0.1; // 10% of deadline

            List<Job>UpwardRankSortedJobs=sortUpwardRanks(sortedJobs, UpwardRanks, taskMap);

            computeEST_EFT(sortedJobs, taskMap, vms, median_bw, fileSizeMap, 
       fileToProducerTaskMap);
            Collections.reverse(sortedJobs);
            computeSlackReverse(sortedJobs,  taskMap, userDeadline, fileSizeMap, 
        fileToProducerTaskMap, median_bw);
        HashMap<VMData,Double>vmAvail=new HashMap<>();
           
           List<VMData>cheapVMs = vms.stream()
                .filter(vm -> vm.mips >= median_mips * 0.5) 
                .sorted((a, b) -> Double.compare(a.costperMIPS, b.costperMIPS))
                .limit((int)(vms.size() * 0.5))
                .collect(Collectors.toList());
            for(VMData vm:vms){
                vmAvail.put(vm,0.0);
            }
            ScheduleResult scheduleDetails=scheduledJobs( UpwardRankSortedJobs,vms,cheapVMs,slackThreshold,
                                     vmAvail, taskMap, fileSizeMap,fileToProducerTaskMap);
            List<Job>scheduledjob=scheduleDetails.schedule;

            rows.add(new String[]{file, String.valueOf(df), String.valueOf(scheduleDetails.totalCost)});

           }

}
            //saveDeadlineCostCSV(deadlineList, costs);
            saveDeadlinesWorkflows("workflow-wise-costs-V6-billingfix.csv",rows);
           } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void saveDeadlinesWorkflows(String fileName, List<String[]> rows) {
    try (PrintWriter writer = new PrintWriter(new File(fileName))) {
        writer.println("Workflow,Deadline,Cost"); // header
        for (String[] row : rows) {
            writer.println(String.join(",", row));
        }
        System.out.println("CSV written to: " + fileName);
    } catch (FileNotFoundException e) {
        System.err.println("Error writing CSV: " + e.getMessage());
    }
}


public static List<Double> generateDeadlines(double start, double end, int points) {
    List<Double> deadlines = new ArrayList<>();
    double gap = (end - start) / (points - 1);
    for (int i = 0; i < points; i++) {
        deadlines.add(start + i * gap);
    }
    return deadlines;
}

    //writing a function to get the mean MIPS from a list of VMs data
   public static double getMedianMIPS(List<VMData> vms) {
    List<Double> mipsList = vms.stream().map(vm -> vm.mips).sorted().collect(Collectors.toList());
    int n = mipsList.size();
    return (n % 2 == 0) ? (mipsList.get(n / 2 - 1) + mipsList.get(n / 2)) / 2.0 : mipsList.get(n / 2);
}

    public static double getMedianBW(List<VMData> vms) {
    List<Double> bwList = vms.stream().map(vm -> vm.networkPerformance).sorted().collect(Collectors.toList());
    int n = bwList.size();
    return (n % 2 == 0) ? (bwList.get(n / 2 - 1) + bwList.get(n / 2)) / 2.0 : bwList.get(n / 2);
}

//     //writing a function to convert it to topological sort
    public static List<String> topologicalSort(Map<String, Job> taskMap) {
        List<String> sortedList = new ArrayList<>();
        Set<String> visited = new HashSet<>();
        Set<String> tempMark = new HashSet<>();

        for (String jobId : taskMap.keySet()) {
            if (!visited.contains(jobId)) {
                topologicalSortUtil(jobId, taskMap, visited, tempMark, sortedList);
            }
        }
        Collections.reverse(sortedList);
        return sortedList;
    }

    public static void topologicalSortUtil(String jobId, Map<String, Job> taskMap, Set<String> visited, Set<String> tempMark, List<String> sortedList) {
        if (tempMark.contains(jobId)) {
            throw new RuntimeException("Cycle detected in the graph");
        }
        if (!visited.contains(jobId)) {
            tempMark.add(jobId);
            for (String child : taskMap.get(jobId).children) {
                topologicalSortUtil(child, taskMap, visited, tempMark, sortedList);
            }
            tempMark.remove(jobId);
            visited.add(jobId);
            sortedList.add(jobId);
        }
    }

    public static Map<String, Double> calculateUpwardRanks(
    List<String> topoSortedJobIds, Map<String, Job> taskMap) {

    Map<String, Double> upwardRanks = new HashMap<>();

    // Process in reverse topological order (from exit to entry)
    for (int i = topoSortedJobIds.size() - 1; i >= 0; i--) {
        String jobId = topoSortedJobIds.get(i);
        Job job = taskMap.get(jobId);

        double maxChildRank = 0;
        for (String childId : job.children) {
            maxChildRank = Math.max(maxChildRank, upwardRanks.getOrDefault(childId, 0.0));
        }

        double rank = job.runtime + maxChildRank;
        upwardRanks.put(jobId, rank);
    }

    return upwardRanks;
}

    public static List<Job> sortUpwardRanks(List<String> sortedJobs, Map<String, Double> UpwardRanks, Map<String,Job>jobMap) {
    // Sort job IDs by descending upward rank
    sortedJobs.sort((a, b) -> Double.compare(UpwardRanks.get(b), UpwardRanks.get(a)));

    // Create and return list of Job objects in that order
    List<Job> sortedJobObjects = new ArrayList<>();
    for (String jobId : sortedJobs) {
        sortedJobObjects.add(jobMap.get(jobId));
    }
    return sortedJobObjects;
}

//V3
public static ScheduleResult scheduledJobs(
        List<Job> UpwardRankSortedJobs,
        List<VMData> vms,
        List<VMData> cheapvms,
        double slackThreshold,
        HashMap<VMData, Double> vmAvailability,
        Map<String, Job> taskMap, 
        Map<String,Double>fileSizeMap, 
        Map<String,String>fileToProducerTaskMap
) {
    List<Job> schedule = new ArrayList<>();
    double totalCost = 0.0;
    final int billingUnit = 3600; // seconds (1 hour)

    Map<VMData, List<Job>> vmTaskMap = new HashMap<>();
    List<VMData> allVMs = new ArrayList<>();
    allVMs.addAll(vms);
    allVMs.addAll(cheapvms);
    for (VMData vm : allVMs) {
        vmTaskMap.put(vm, new ArrayList<>());
    }

    for (Job task : UpwardRankSortedJobs) {
        double minCost = Double.MAX_VALUE;
        VMData bestVM = null;
        double bestEST = 0.0;
        double bestEFT = 0.0;

        List<VMData> candidateVMs = (task.slack > slackThreshold) ? cheapvms : vms;
       
        for (VMData vm : candidateVMs) {
            double est = vmAvailability.getOrDefault(vm, 0.0);
            List<Job>existingJobs=vmTaskMap.get(vm);
            double dataReadyTime = 0.0;
            for (String parentId : task.parents) {
                Job parentJob = taskMap.get(parentId);
                if (parentJob == null || parentJob.endTime == 0.0) continue;
                double commTime = 0.0;
                if (parentJob.assignedVM != vm) {
                    double dataSizeMB = getDataSizeTransferred(parentJob, task,fileSizeMap,fileToProducerTaskMap);
                    double bandwidthMBps = vm.networkPerformance * 125.0;
                    commTime = dataSizeMB / bandwidthMBps;
                }
                est = Math.max(est, parentJob.endTime + commTime);
                dataReadyTime = Math.max(dataReadyTime, parentJob.endTime + commTime);
            }
             est = Math.max(dataReadyTime, est);
            double runtime = task.mi / vm.mips;
            double eft = est + runtime;
            double bestDelayBias = Double.MAX_VALUE;


           if (eft <= task.subDeadline) {
    double cost = (Math.ceil(runtime / billingUnit) * billingUnit / 3600.0) * vm.cost;

    double delayAfterDataReady = Math.max(0.0, est - dataReadyTime);
    double normalizedDelay = delayAfterDataReady / task.subDeadline;

    if (cost < minCost || (cost == minCost && normalizedDelay < bestDelayBias)) {
        minCost = cost;
        bestDelayBias = normalizedDelay;
        bestVM = vm;
        bestEST = est;
        bestEFT = eft;
    }
}

        }

        if (bestVM != null) {
            task.assignedVM = bestVM;
            task.startTime = bestEST;
            task.endTime = bestEFT;

            vmAvailability.put(bestVM, bestEFT);
            vmTaskMap.get(bestVM).add(task);
            schedule.add(task);
        } else {
            //System.err.println("Deadline constraint cannot be met for task: " + task.id);
        }
    }

       // ✅ Accurately compute total cost based on per-VM usage window
    // double totalCost = 0.0;
   for (Map.Entry<VMData, List<Job>> entry : vmTaskMap.entrySet()) {
    VMData vm = entry.getKey();
    List<Job> jobsOnVM = entry.getValue();
    if (jobsOnVM.isEmpty()) continue;

    double vmStart = jobsOnVM.stream().mapToDouble(j -> j.startTime).min().getAsDouble();
    double vmEnd = jobsOnVM.stream().mapToDouble(j -> j.endTime).max().getAsDouble();
    double usageTimeInSeconds = vmEnd - vmStart;

    double perSecondRate = vm.cost / 3600.0;
    double vmCost = usageTimeInSeconds * perSecondRate;

    totalCost += vmCost;
}




    System.out.println("Total optimized cost: $" + totalCost);
    return new ScheduleResult(schedule, totalCost);
}

//function to get total data transfered from parent to child task
public static double getDataSizeTransferred(Job producer, Job consumer, Map<String,Double>fileSizeMap, Map<String,String>fileToProducerTaskMap) {
    List<String> outputFiles = producer.outputFiles;
    List<String> inputFiles = consumer.inputFiles;

    double totalTransferred = 0.0;
    for (String file : inputFiles) {
         if (fileToProducerTaskMap.containsKey(file))  {
            totalTransferred += fileSizeMap.getOrDefault(file, 0.0);
        }
    }
    return totalTransferred;
}

public static void computeSlackReverse(List<String> reverseTopologicalTasks, Map<String, Job> taskMap, double globalDeadline, Map<String,Double>fileSizeMap, 
        Map<String,String>fileToProducerTaskMap, double AvgBandwidth) {
    for (String taskId : reverseTopologicalTasks) {
        double lft = globalDeadline;
        Job task=taskMap.get(taskId);
        for (String childId : task.children) {
            Job child = taskMap.get(childId);
            double commTime = 0.0;

            if (child != null) {
                double dataSize = getDataSizeTransferred(task, child,fileSizeMap,fileToProducerTaskMap);
                commTime = dataSize / AvgBandwidth;
                lft = Math.min(lft, child.startTime - commTime);
            }
        }

        task.endTime = (task.children.isEmpty()) ? globalDeadline : lft;
        task.slack = task.endTime - task.startTime;
    }
}

public static void computeEST_EFT(List<String> topoSortedTasks, Map<String, Job> taskMap, List<VMData> vms, double meanBandwidthMBps, Map<String,Double>fileSizeMap, 
        Map<String,String>fileToProducerTaskMap) {
    for (String taskId : topoSortedTasks) {
        double est = 0.0;
        Job task=taskMap.get(taskId);
        for (String parentId : task.parents) {
            Job parent = taskMap.get(parentId);
            double commTime = 0.0;

            if (parent != null) {
                double dataSize = getDataSizeTransferred(parent, task,fileSizeMap,fileToProducerTaskMap);
                commTime = dataSize / meanBandwidthMBps;
                est = Math.max(est, parent.endTime + commTime);
            }
        }

        double runtime = task.mi / getMedianMIPS(vms);
        task.startTime = est;
        task.endTime = est + runtime;
    }
}


    public static void saveDeadlineCostCSV(List<Double> deadlines, List<Double> costs) {
        try (PrintWriter writer = new PrintWriter(new File("costs_data_V1.csv"))) {
            writer.println("Deadline,Cost"); // CSV Header

            for (int i = 0; i < deadlines.size(); i++) {
                writer.println(deadlines.get(i) + "," + costs.get(i));
            }

            System.out.println("Saved to " + "costs_data_V1.csv");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}