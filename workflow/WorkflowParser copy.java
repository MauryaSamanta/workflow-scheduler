import javax.xml.parsers.*;
import org.w3c.dom.*;
import java.io.File;
import java.io.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WorkflowParser {
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
            for(double userDeadline:deadlineList)
           {
            File xmlFile = new File("Inspiral_50.xml"); // Replace with your file
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
        String linkType = usesElement.getAttribute("link"); // "input" or "output"
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

           //System.out.println("entryRank="+entryRank);


          
          
           HashMap<VMData,Double>vmAvail=new HashMap<>();
           //List<VMData>vms=VMData.parseCSV("cleaned_vm_data.csv"); 
           List<VMData>cheapVMs = vms.stream()
                .sorted((a, b) -> Double.compare(a.costperMIPS, b.costperMIPS))
                .limit((int)(vms.size() * 0.5))
                .collect(Collectors.toList());
            for(VMData vm:vms){
                vmAvail.put(vm,0.0);
            }
            ScheduleResult scheduleDetails=scheduledJobs( UpwardRankSortedJobs,vms,cheapVMs,slackThreshold,
                                     vmAvail, taskMap, fileSizeMap,fileToProducerTaskMap);
            List<Job>scheduledjob=scheduleDetails.schedule;
            costs.add(scheduleDetails.totalCost);
           }
//             for (Job scheduled : scheduledjob) {
//     System.out.println("Task ID: " + scheduled.id);
//     System.out.println("Parents: " + scheduled.parents);
//     System.out.println("Start Time: " + scheduled.startTime);
//     System.out.println("End Time: " + scheduled.endTime);
//     System.out.println("Assigned VM: " + (scheduled.assignedVM != null ? scheduled.assignedVM.id : "None"));
//     System.out.println("-----------");
// }

            saveDeadlineCostCSV(deadlineList, costs);
           

           
           

            

        } catch (Exception e) {
            e.printStackTrace();
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

//scheduler function using IC-PCP algorithm

//     public static List<Job> scheduledJobs(List<Job> UpwardRankSortedJobs, List<VMData> vms,List<VMData> cheapvms, double slackThreshold
//                                       HashMap<VMData, Double> vmAvailability, Map<String, Job> taskMap) {
//     List<Job> schedule = new ArrayList<>();
//     double totalCost = 0.0;

//     for (Job task : UpwardRankSortedJobs) {
//         double minCost = Double.MAX_VALUE;
//         VMData bestVM = null;
//         double bestEST = 0.0;
//         double bestEFT = 0.0;

//         for (VMData vm : vms) {
//             double avail = vmAvailability.getOrDefault(vm, 0.0);
//             double est = avail;

//             // Calculate EST based on parent dependencies
//             for (String parentId : task.parents) {
//                 Job parentJob = taskMap.get(parentId);
//                 if (parentJob == null || parentJob.endTime == 0.0) continue;

//                 double parentEnd = parentJob.endTime;
//                 double commTime = (parentJob.assignedVM == vm) ? 0.0 : 1.0;
//                 est = Math.max(est, parentEnd + commTime);
//             }

//             double runtime = task.mi / vm.mips;
//             double eft = est + runtime;

//             if (eft <= task.subDeadline) {
//                 double cost = (runtime / 3600.0) * vm.cost;
//                 if (cost < minCost) {
//                     minCost = cost;
//                     bestVM = vm;
//                     bestEST = est;
//                     bestEFT = eft;
//                 }
//             }
//         }

//         // Final assignment if any valid VM was found
//         if (bestVM != null) {
//             task.assignedVM = bestVM;
//             task.startTime = bestEST;
//             task.endTime = bestEFT;
//             vmAvailability.put(bestVM, bestEFT);
//             totalCost += minCost;
//             schedule.add(task);
//         } else {
//             System.err.println("Deadline constraint cannot be met for task: " + task.id);
//         }
//     }

//   System.out.println("Total optimized cost: $" + String.format("%.6f", totalCost));

//     return schedule;
// }
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
            }

            double runtime = task.mi / vm.mips;
            double eft = est + runtime;

            if (eft <= task.subDeadline) {
                double cost = (Math.ceil(runtime / billingUnit) * billingUnit / 3600.0) * vm.cost;

                // Calculate idle time gap for packing effectiveness
                List<Job> existingJobs = vmTaskMap.get(vm);
                double lastEnd = existingJobs.isEmpty() ? 0.0 : existingJobs.get(existingJobs.size() - 1).endTime;
                double idleGap = Math.max(0.0, est - lastEnd);

                // Bias packing by preferring smaller idle gaps (but no fake cost reduction)
                if (cost < minCost || (cost == minCost && idleGap < (bestEST - lastEnd))) {
                    minCost = cost;
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

    // billing per VM
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







//     //scheduler function
//     public static List<ScheduledTask> scheduleJobs(List<String> sortedJobs, Map<String, Job> taskMap, List<VM> vms) {
//     List<ScheduledTask> schedule = new ArrayList<>();
//     Map<String, ScheduledTask> taskExecution = new HashMap<>();

//     for (String jobId : sortedJobs) {
//         Job job = taskMap.get(jobId);

//         double bestEFT = Double.MAX_VALUE;
//         VM bestVM = null;
//         double bestStartTime = 0;

//         for (VM vm : vms) {
//             // Compute Earliest Start Time (EST)
//             double est = vm.availableAt;

//             for (String parentId : job.parents) {
//                 ScheduledTask parentTask = taskExecution.get(parentId);
//                 double commTime = parentTask.vmId.equals(vm.id) ? 0.0 : 1.0;
//                 est = Math.max(est, parentTask.endTime + commTime);
//             }

//             double eft = est + job.runtime;

//             if (eft < bestEFT) {
//                 bestEFT = eft;
//                 bestStartTime = est;
//                 bestVM = vm;
//             }
//         }

//         // Assign task to best VM
//         ScheduledTask scheduled = new ScheduledTask(jobId, bestVM.id, bestStartTime, bestEFT);
//         taskExecution.put(jobId, scheduled);
//         schedule.add(scheduled);

//         // Update VM availability
//         bestVM.availableAt = bestEFT;
//     }

//     return schedule;
// }

//V4 OF SCHEDULER
// public static List<Job> scheduledJobs(List<Job> UpwardRankSortedJobs, List<VMData> vms,
//                                       List<VMData> cheapvms, double slackThreshold,
//                                       HashMap<VMData, Double> vmAvailability, Map<String, Job> taskMap) {

//     List<Job> schedule = new ArrayList<>();
//     double totalCost = 0.0;

//     for (Job task : UpwardRankSortedJobs) {
//         double minCost = Double.MAX_VALUE;
//         VMData bestVM = null;
//         double bestEST = 0.0;
//         double bestEFT = 0.0;

//         List<VMData> prioritizedVMs = new ArrayList<>(vms);
//         prioritizedVMs.sort(Comparator.comparingDouble(vmAvailability::get)); // Prioritize VMs with earlier availability

//         for (VMData vm : prioritizedVMs) {
//             double avail = vmAvailability.getOrDefault(vm, 0.0);
//             double est = avail;

//             for (String parentId : task.parents) {
//                 Job parentJob = taskMap.get(parentId);
//                 if (parentJob == null || parentJob.endTime == 0.0) continue;

//                 double parentEnd = parentJob.endTime;
//                 double commTime = (parentJob.assignedVM == vm) ? 0.0 : 1.0;
//                 est = Math.max(est, parentEnd + commTime);
//             }

//             double runtime = task.mi / vm.mips;
//             double eft = est + runtime;

//             if (eft <= task.subDeadline) {
//                 double cost = (runtime / 3600.0) * vm.cost;

//                 // 🔥 Prioritize reusing idle VMs if cost difference is not drastic
//                 if (vmAvailability.containsKey(vm)) {
//                     cost *= 0.9; // Boost reused VM preference
//                 }

//                 if (cost < minCost) {
//                     minCost = cost;
//                     bestVM = vm;
//                     bestEST = est;
//                     bestEFT = eft;
//                 }
//             }
//         }

//         if (bestVM != null) {
//             task.assignedVM = bestVM;
//             task.startTime = bestEST;
//             task.endTime = bestEFT;
//             vmAvailability.put(bestVM, bestEFT);
//             totalCost += minCost;
//             schedule.add(task);
//         } else {
//             System.err.println("Deadline constraint cannot be met for task: " + task.id);
//         }
//     }

//     System.out.println("Total optimized cost: $" +  totalCost);
//     return schedule;
// }




}
