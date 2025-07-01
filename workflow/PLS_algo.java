import javax.xml.parsers.*;
import org.w3c.dom.*;
import java.io.File;
import java.io.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PLS_algo {
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
            List<String> files=Arrays.asList("CyberShake_30.xml", "Epigenomics_46.xml", "Inspiral_30.xml", "Inspiral_50.xml", "Inspiral_30.xml",
            "Montage_50.xml","Sipht_30.xml");
            double dfactors[]={1.0, 1.1,1,2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 2.0};
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

           Map<String,Double>ProbaUpwardRanks= calculateProbabilisticRanks(sortedJobs, taskMap, fileSizeMap, fileToProducerTaskMap,
median_bw,median_mips);
            double entryRank=0.0;
           for(Map.Entry<String,Double>entry:ProbaUpwardRanks.entrySet()){
            double rank=entry.getValue();
            entryRank=Math.max(rank,entryRank);
           
           }
           double userDeadline=df*entryRank;
           
           for( Map.Entry<String,Job>entry:taskMap.entrySet()){
            Job current = entry.getValue();
            double pr_i = ProbaUpwardRanks.get(current.id); // Use probabilistic ranks instead
            double runtime = current.mi / median_mips;
            current.subDeadline = userDeadline * ((entryRank - pr_i + runtime) / entryRank);
           }
             double slackThreshold = userDeadline * 0.1; // 10% of deadline

            List<Job>UpwardRankSortedJobs=sortProbabilisticRanks(sortedJobs, ProbaUpwardRanks, taskMap);

    //         computeEST_EFT(sortedJobs, taskMap, vms, median_bw, fileSizeMap, 
    //    fileToProducerTaskMap);
    //         Collections.reverse(sortedJobs);
    //         computeSlackReverse(sortedJobs,  taskMap, userDeadline, fileSizeMap, 
    //     fileToProducerTaskMap, median_bw);
        HashMap<VMData,Double>vmAvail=new HashMap<>();
           
           List<VMData>cheapVMs = vms.stream()
                .filter(vm -> vm.mips >= median_mips * 0.5) 
                .sorted((a, b) -> Double.compare(a.costperMIPS, b.costperMIPS))
                .limit((int)(vms.size() * 0.5))
                .collect(Collectors.toList());
            for(VMData vm:vms){
                vmAvail.put(vm,0.0);
            }
            ScheduleResult scheduleDetails=scheduledJobs_PLS( UpwardRankSortedJobs,vms,cheapVMs,slackThreshold,
                                     vmAvail, taskMap, fileSizeMap,fileToProducerTaskMap);
            List<Job>scheduledjob=scheduleDetails.schedule;

            rows.add(new String[]{file, String.valueOf(df), String.valueOf(scheduleDetails.totalCost)});

           }
//             for (Job scheduled : scheduledjob) {
//     System.out.println("Task ID: " + scheduled.id);
//     System.out.println("Parents: " + scheduled.parents);
//     System.out.println("Start Time: " + scheduled.startTime);
//     System.out.println("End Time: " + scheduled.endTime);
//     System.out.println("Assigned VM: " + (scheduled.assignedVM != null ? scheduled.assignedVM.id : "None"));
//     System.out.println("-----------");
// }
}
            //saveDeadlineCostCSV(deadlineList, costs);
            saveDeadlinesWorkflows("workflow-wise-costs-PLS-V2.csv",rows);
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

    public static Map<String, Double> calculateProbabilisticRanks(
    List<String> topoSortedJobIds,
    Map<String, Job> taskMap,
    Map<String, Double> fileSizeMap,
    Map<String, String> fileToProducerTaskMap,
    double avgBandwidth,
    double avgMIPS
) {
    Map<String, Double> priMap = new HashMap<>();
    Random random = new Random();
    double theta = 1.5;

    // Process in reverse topological order (from exit to entry)
    for (int i = topoSortedJobIds.size() - 1; i >= 0; i--) {
        String jobId = topoSortedJobIds.get(i);
        Job job = taskMap.get(jobId);

        double maxChildPri = 0.0;
        for (String childId : job.children) {
            Job child = taskMap.get(childId);
            double commTime = getDataSizeTransferred(job, child, fileSizeMap, fileToProducerTaskMap) / avgBandwidth;

            double ccrj = child.mi / Math.max(commTime, 1e-6); // Avoid division by 0

            double randVal = random.nextDouble(); // [0.0, 1.0)
            boolean includeComm = randVal >= (1 - (1 / ccrj)) / theta;

            double childRank = priMap.getOrDefault(childId, 0.0);
            double total = childRank + (includeComm ? commTime : 0.0);
            maxChildPri = Math.max(maxChildPri, total);
        }

        double rank = (job.mi / avgMIPS) + maxChildPri;
        priMap.put(jobId, rank);
    }

    return priMap;
}


   public static List<Job> sortProbabilisticRanks(List<String> sortedJobs, Map<String, Double> ProbRanks, Map<String, Job> jobMap) {
    // Sort job IDs by descending probabilistic rank
    sortedJobs.sort((a, b) -> Double.compare(ProbRanks.get(b), ProbRanks.get(a)));

    // Create and return list of Job objects in that order
    List<Job> sortedJobObjects = new ArrayList<>();
    for (String jobId : sortedJobs) {
        sortedJobObjects.add(jobMap.get(jobId));
    }
    return sortedJobObjects;
}


//ACCORDING TO PLS algo
public static ScheduleResult scheduledJobs_PLS(
        List<Job> ProbRankSortedJobs,
        List<VMData> vms,
        List<VMData> cheapvms,
        double slackThreshold,
        HashMap<VMData, Double> vmAvailability,
        Map<String, Job> taskMap,
        Map<String, Double> fileSizeMap,
        Map<String, String> fileToProducerTaskMap
) {
    List<Job> schedule = new ArrayList<>();
    double totalCost = 0.0;
    final int billingUnit = 3600;

    Map<VMData, List<Job>> vmTaskMap = new HashMap<>();
    for (VMData vm : vms) vmTaskMap.put(vm, new ArrayList<>());

    for (Job task : ProbRankSortedJobs) {
        VMData bestVM = null;
        double bestEST = 0.0, bestEFT = Double.MAX_VALUE;
        double minCostIncrement = Double.MAX_VALUE;

        for (VMData vm : vms) {
            double est = vmAvailability.getOrDefault(vm, 0.0);
            double dataReadyTime = 0.0;

            for (String parentId : task.parents) {
                Job parent = taskMap.get(parentId);
                if (parent == null || parent.endTime == 0.0) continue;

                double commTime = 0.0;
                if (parent.assignedVM != vm) {
                    double dataSizeMB = getDataSizeTransferred(parent, task, fileSizeMap, fileToProducerTaskMap);
                    double bandwidthMBps = vm.networkPerformance * 125.0;
                    commTime = dataSizeMB / bandwidthMBps;
                }
                est = Math.max(est, parent.endTime + commTime);
                dataReadyTime = Math.max(dataReadyTime, parent.endTime + commTime);
            }

            est = Math.max(est, dataReadyTime);
            double runtime = task.mi / vm.mips;
            double eft = est + runtime;

            if (eft <= task.subDeadline) {
                // Cost increment = new cost for this VM
                List<Job> existing = vmTaskMap.get(vm);
                double prevEnd = existing.isEmpty() ? 0.0 : existing.get(existing.size() - 1).endTime;
                double prevBilling = Math.ceil((prevEnd - (existing.isEmpty() ? 0 : existing.get(0).startTime)) / billingUnit) * billingUnit;
                double newBilling = Math.ceil((Math.max(prevEnd, eft) - (existing.isEmpty() ? est : existing.get(0).startTime)) / billingUnit) * billingUnit;

                double costInc = ((newBilling - prevBilling) / 3600.0) * vm.cost;

                if (costInc < minCostIncrement || (costInc == minCostIncrement && eft < bestEFT)) {
                    minCostIncrement = costInc;
                    bestVM = vm;
                    bestEST = est;
                    bestEFT = eft;
                }
            }
        }

        //  If no VM meets sub-deadline, fallback to VM with min EFT
        if (bestVM == null) {
            for (VMData vm : vms) {
                double est = vmAvailability.getOrDefault(vm, 0.0);
                double dataReadyTime = 0.0;

                for (String parentId : task.parents) {
                    Job parent = taskMap.get(parentId);
                    if (parent == null || parent.endTime == 0.0) continue;

                    double commTime = 0.0;
                    if (parent.assignedVM != vm) {
                        double dataSizeMB = getDataSizeTransferred(parent, task, fileSizeMap, fileToProducerTaskMap);
                        double bandwidthMBps = vm.networkPerformance * 125.0;
                        commTime = dataSizeMB / bandwidthMBps;
                    }
                    est = Math.max(est, parent.endTime + commTime);
                    dataReadyTime = Math.max(dataReadyTime, parent.endTime + commTime);
                }

                est = Math.max(est, dataReadyTime);
                double runtime = task.mi / vm.mips;
                double eft = est + runtime;

                if (eft < bestEFT) {
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
        }
    }

    // Final cost computation per VM
    for (Map.Entry<VMData, List<Job>> entry : vmTaskMap.entrySet()) {
        List<Job> jobs = entry.getValue();
        if (jobs.isEmpty()) continue;

        jobs.sort(Comparator.comparingDouble(j -> j.startTime));
        double start = jobs.get(0).startTime;
        double end = jobs.get(jobs.size() - 1).endTime;
        double billedDuration = Math.ceil((end - start) / billingUnit) * billingUnit;

        double billedCost = (billedDuration / 3600.0) * entry.getKey().cost;
        totalCost += billedCost;
    }

    System.out.println("PLS cost: $" + totalCost);
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
