import java.io.*;
import java.util.*;
import java.util.stream.Collectors;
import javax.xml.parsers.*;
import org.w3c.dom.*;

public class WorkflowParser {
    final  static double REQUIRED_RELIABILITY = 0.99;
final static double LAMBDA = 0.001;  
static Random random = new Random(42);
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
    List<VMData>eligibleVMS;
    public Job(String id, double runtime, double mi,List<String>inputFiles,List<String>outputFiles) {
        this.id = id;
        this.runtime = runtime;
        this.mi=mi;
        subDeadline=0.0;
        slack=0.0;
        this.inputFiles=inputFiles;
        this.outputFiles=outputFiles;
    }

    public Job(Job other) {
    this.id = other.id;
    this.mi = other.mi;
    this.parents = new ArrayList<>(other.parents); // deep copy
    this.subDeadline = other.subDeadline;
    this.slack = other.slack;

    // These will be re-set for replicas
    this.startTime = 0.0;
    this.endTime = 0.0;
    this.assignedVM = null;

    // this.isReplica = true; // mark it as replica
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

        // Scanner sc=new Scanner(System.in);
        // System.out.println("Enter deadline(in miliseconds):");
        // int userDeadline=sc.nextInt();
        try {
           
            List<String> files=Arrays.asList("Montage_100.xml","Epigenomics_100.xml","Sipht_100.xml");
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
            List<VMData>vms=VMData.parseCSV("cleaned_vm_data_storage.csv"); 
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
            // System.out.println(current.subDeadline);
            //current.slack = current.subDeadline - UpwardRanks.get(current.id);
           }
             double slackThreshold = userDeadline * 0.1; // 10% of deadline

            List<Job>UpwardRankSortedJobs=sortUpwardRanks(sortedJobs, UpwardRanks, taskMap);
            getEligibleVMs(UpwardRankSortedJobs, vms, fileSizeMap);

            double[] replicaCounts = optimizeReplicaCounts(
    UpwardRankSortedJobs, 
    vms, 
    LAMBDA, 
    userDeadline, 
    0.99999
    
);

        
        // for(double replica:replicaCounts){
        //     System.out.println(replica);
        // }
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
                                     vmAvail, taskMap, fileSizeMap,fileToProducerTaskMap,replicaCounts);
            List<Job>scheduledjob=scheduleDetails.schedule;

            rows.add(new String[]{file, String.valueOf(df), String.valueOf(scheduleDetails.totalCost)});
    //     for(Job job:UpwardRankSortedJobs)
    //     {
    //         if(job.eligibleVMS.isEmpty())
    //         System.out.println(job.id);
    //     }

           }

}
            //saveDeadlineCostCSV(deadlineList, costs);
            saveDeadlinesWorkflows("Random_Legrange_v1.csv",rows);
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
        Map<String,String>fileToProducerTaskMap,
            double[] replicaCounts
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
    int taskIndex = 0;
    for (Job task : UpwardRankSortedJobs) {
        double minCost = Double.MAX_VALUE;
        VMData bestVM = null;
        double bestEST = 0.0;
        double bestEFT = 0.0;

        List<VMData> candidateVMs = task.eligibleVMS;
       
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
    double presentreliability=getReliability(task.mi/bestVM.mips, LAMBDA);
    int numReplicas = (int) Math.round(replicaCounts[taskIndex]);  // x[i] from optimization
    if (numReplicas < 1) numReplicas = 1; // at least 1 main instance

    List<VMData> selectedVMs = new ArrayList<>();
    selectedVMs.add(bestVM);  // Main instance

    // Update main task times
    task.assignedVM = bestVM;
    task.startTime = bestEST;
    task.endTime = bestEFT;
    vmAvailability.put(bestVM, bestEFT);
    vmTaskMap.get(bestVM).add(task);  // add main task
    schedule.add(task);               // add main to schedule

    // Select replica VMs
    List<VMData> candidateReplicas = new ArrayList<>(task.eligibleVMS);
    candidateReplicas.remove(bestVM);

    candidateReplicas.sort(Comparator.comparingDouble(vm -> {
        double runtime = task.mi / vm.mips;
        double reliability = getReliability(runtime, LAMBDA);
        double cost = (runtime / 3600.0) * vm.cost;
        return cost / reliability;
    }));

    int replicasAdded = 1;
    boolean successAchieved = false;
    for (VMData replicaVM : candidateReplicas) {
        if (replicasAdded >= numReplicas|| successAchieved) break;
        // if(presentreliability>=0.90000)
        //     break;
        // presentreliability=presentreliability*getReliability(task.mi/replicaVM.mips, LAMBDA);
        double est = vmAvailability.getOrDefault(replicaVM, 0.0);
        double runtime = task.mi / replicaVM.mips;
        double eft = est + runtime;

        if (eft > task.subDeadline) continue;  // respect deadline

        // Clone the task or mark as replica
        Job replica = new Job(task);

        replica.assignedVM = replicaVM;
        replica.startTime = est;
        replica.endTime = eft;
        // replica.isReplica = true;  // optional field you can define

        vmAvailability.put(replicaVM, eft);
        vmTaskMap.get(replicaVM).add(replica);
        schedule.add(replica);

        replicasAdded++;
         double randomValue = random.nextDouble();

        if (0.9999 <= randomValue) {
        successAchieved = true;
        }
    }
}
taskIndex++;


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

public static double getReliability(double runtime, double lambda) {
    // System.out.println(Math.exp(-lambda * runtime));
    return Math.exp(-lambda * runtime);
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
    return totalTransferred/1024;
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

//     public static double[] optimizeReplicaCounts(List<Job> jobs, List<VMData> vms, double lambda, double deadline, double requiredReliability) {
//     int n = jobs.size();
//     double[] x = new double[n];
//     Arrays.fill(x, 1.0); // Start with one replica per job

//     double learningRate = 0.1;
//     int maxIters = 1000;

//     for (int iter = 0; iter < maxIters; iter++) {
//         double cost = 0;
//         double logReliability = 0;

//         for (int i = 0; i < n; i++) {
//             Job job = jobs.get(i);
//             double t_i = job.runtime;
//             double c_i = getCheapestCost(vms);
//             double f_i = 1.0 - getReliability(t_i, lambda);

//             cost += c_i * t_i * x[i];
//             logReliability += Math.log(1 - Math.pow(f_i, x[i]));
//         }

//         double constraint1 = logReliability - Math.log(requiredReliability);

//         // Gradient descent with penalty
//         for (int i = 0; i < n; i++) {
//             Job job = jobs.get(i);
//             double t_i = job.mi / getFastestMIPS(vms);
//             double c_i = getCheapestCost(vms);
//             double f_i = 1.0 - getReliability(t_i, lambda);

//             double dReliability = -Math.pow(f_i, x[i]) * Math.log(f_i) / (1e-8 + 1 - Math.pow(f_i, x[i]));
// double grad = c_i * t_i + 1000 * dReliability * constraint1;
// x[i] -= learningRate * grad;

// if (x[i] < 1.0) x[i] = 1.0;
// if (x[i] > 10.0) x[i] = 10.0;

//         }
//     }

//     return x;
// }
// public static double[] optimizeReplicaCounts(List<Job> jobs, List<VMData> vms, double lambda, double deadline, double requiredReliability) {
//     int n = jobs.size();
//     double[] x = new double[n];
    
//     // Initialize based on reliability requirement
//     double baseReplicas = Math.max(1.0, -Math.log(requiredReliability) / Math.log(0.9));
//     Arrays.fill(x, baseReplicas);
    
//     // Simple heuristic: more critical tasks (higher upward rank) get more replicas
//     for (int i = 0; i < n; i++) {
//         Job job = jobs.get(i);
//         double reliability = getReliability(job.runtime, lambda);
        
//         // Calculate minimum replicas needed for this reliability
//         double failureRate = 1.0 - reliability;
//         double minReplicas = Math.log(1.0 - requiredReliability) / Math.log(failureRate);
        
//         x[i] = Math.max(1.0, Math.ceil(minReplicas));
//     }
    
//     return x;
// }


public static double[] optimizeReplicaCounts(List<Job> jobs, List<VMData> vms, double lambda, double deadline, double requiredReliability) {
    int n = jobs.size();
    double[] x = new double[n];
    
    // Better initialization: estimate minimum replicas needed using only eligible VMs
    for (int i = 0; i < n; i++) {
        Job job = jobs.get(i);
        
        // Use only eligible VMs for this job
        if (job.eligibleVMS == null || job.eligibleVMS.isEmpty()) {
            // System.out.println("Warning: Job " + job.id + " has no eligible VMs");
            x[i] = 1.0;
            continue;
        }
        
        // Get fastest MIPS from eligible VMs only
        double fastestMIPS = 0;
        for (VMData vm : job.eligibleVMS) {
            fastestMIPS = Math.max(fastestMIPS, vm.mips);
        }
        
        double runtime = job.mi / fastestMIPS;
        double singleTaskReliability = getReliability(runtime, lambda);
        
        // Calculate minimum replicas needed for individual task reliability
        // For system reliability R_sys = product R_i, we need each R_i ≥ R_sys^(1/n)
        double targetTaskReliability = Math.pow(requiredReliability, 1.0 / n);
        
        if (singleTaskReliability >= targetTaskReliability) {
            x[i] = 1.0;
        } else {
            // Solve: 1 - (1-p)^k = targetTaskReliability
            // k = ln(1 - targetTaskReliability) / ln(1 - p)
            double failureRate = 1.0 - singleTaskReliability;
            if (failureRate > 0 && failureRate < 1) {
                x[i] = Math.max(1.0, Math.ceil(Math.log(1.0 - targetTaskReliability) / Math.log(failureRate)));
            } else {
                x[i] = 1.0;
            }
        }
    }
    
    // System.out.println("Initial replica counts based on target reliability (using eligible VMs):");
    // for (int i = 0; i < n; i++) {
    //     System.out.printf("Job %s: %.2f replicas (eligible VMs: %d)\n", 
    //                      jobs.get(i).id, x[i], jobs.get(i).eligibleVMS.size());
    // }
    
    // Check if initial solution meets requirement
    double initialReliability = calculateSystemReliability(x, jobs, lambda);
    // System.out.printf("Initial system reliability: %.6f (required: %.6f)\n", initialReliability, requiredReliability);
    
    if (initialReliability >= requiredReliability) {
        // System.out.println("Initial solution already meets reliability requirement");
        return x;
    }
    
    // If not, use iterative improvement
    double mu = 1.0;
    double learningRate = 0.05;
    double muLearningRate = 0.1;
    int maxIters = 1000;
    
    // Pre-calculate VM characteristics for each job using only eligible VMs
    double[] cheapestCosts = new double[n];
    double[] fastestMIPS = new double[n];
    
    for (int i = 0; i < n; i++) {
        Job job = jobs.get(i);
        
        if (job.eligibleVMS == null || job.eligibleVMS.isEmpty()) {
            // Fallback if no eligible VMs (shouldn't happen if properly filtered)
            cheapestCosts[i] = getCheapestCost(vms);
            fastestMIPS[i] = getFastestMIPS(vms);
            continue;
        }
        
        double minCost = Double.MAX_VALUE;
        double maxMIPS = 0;
        
        // Only consider eligible VMs for this job
        for (VMData vm : job.eligibleVMS) {
            double runtime = job.mi / vm.mips;
            if (runtime <= job.subDeadline) {
                minCost = Math.min(minCost, vm.cost);
                maxMIPS = Math.max(maxMIPS, vm.mips);
            }
        }
        
        // If no eligible VM meets deadline, use cheapest and fastest from eligible VMs
        if (minCost == Double.MAX_VALUE) {
            minCost = Double.MAX_VALUE;
            maxMIPS = 0;
            for (VMData vm : job.eligibleVMS) {
                minCost = Math.min(minCost, vm.cost);
                maxMIPS = Math.max(maxMIPS, vm.mips);
            }
        }
        
        cheapestCosts[i] = minCost;
        fastestMIPS[i] = maxMIPS;
    }
    
    for (int iter = 0; iter < maxIters; iter++) {
        // Calculate current system reliability
        double systemReliability = 1.0;
        double totalCost = 0.0;
        
        for (int i = 0; i < n; i++) {
            Job job = jobs.get(i);
            double runtime = job.mi / fastestMIPS[i];
            double singleTaskReliability = getReliability(runtime, lambda);
            double taskReliability = 1.0 - Math.pow(1.0 - singleTaskReliability, x[i]);
            
            systemReliability *= taskReliability;
            totalCost += cheapestCosts[i] * runtime * x[i];
        }
        
        // Constraint: systemReliability >= requiredReliability
        double constraintViolation = requiredReliability - systemReliability;
        
        // Only update if constraint is violated
        if (constraintViolation > 1e-6) {
            // Update replica counts
            for (int i = 0; i < n; i++) {
                Job job = jobs.get(i);
                double runtime = job.mi / fastestMIPS[i];
                double singleTaskReliability = getReliability(runtime, lambda);
                double failureRate = 1.0 - singleTaskReliability;
                
                if (failureRate > 1e-10 && failureRate < 1.0) {
                    // Cost gradient
                    double costGradient = cheapestCosts[i] * runtime;
                    
                    // Reliability gradient: d(systemReliability)/dx[i]
                    double currentTaskReliability = 1.0 - Math.pow(failureRate, x[i]);
                    double reliabilityGradient = 0.0;
                    
                    if (currentTaskReliability > 1e-10) {
                        reliabilityGradient = systemReliability * Math.pow(failureRate, x[i]) * Math.log(failureRate) / currentTaskReliability;
                    }
                    
                    // Update with penalty method
                    double penalty = 1000.0 * constraintViolation;
                    double totalGradient = costGradient - penalty * reliabilityGradient;
                    
                    x[i] -= learningRate * totalGradient;
                    x[i] = Math.max(1.0, Math.min(10.0, x[i]));
                }
            }
            
            // Update penalty multiplier
            mu = Math.max(0.0, mu + muLearningRate * constraintViolation);
        } else {
            // Constraint satisfied, try to minimize cost
            break;
        }
        
        // Print progress
        // if (iter % 100 == 0) {
        //     System.out.printf("Iter %d: Cost=%.2f, Reliability=%.6f, Violation=%.6f\n", 
        //                     iter, totalCost, systemReliability, constraintViolation);
        // }
        
        // Check convergence
        if (Math.abs(constraintViolation) < 1e-6) {
            // System.out.println("Converged at iteration " + iter);
            break;
        }
    }
    
    // Final verification
    double finalReliability = calculateSystemReliability(x, jobs, lambda);
    // System.out.printf("Final system reliability: %.6f (required: %.6f)\n", finalReliability, requiredReliability);
    
    return x;
}

// Updated helper method to calculate system reliability using eligible VMs
public static double calculateSystemReliability(double[] replicaCounts, List<Job> jobs, double lambda) {
    double systemReliability = 1.0;
    
    for (int i = 0; i < jobs.size(); i++) {
        Job job = jobs.get(i);
        
        // Get fastest MIPS from eligible VMs only
        double fastestMIPS = 0;
        if (job.eligibleVMS != null && !job.eligibleVMS.isEmpty()) {
            for (VMData vm : job.eligibleVMS) {
                fastestMIPS = Math.max(fastestMIPS, vm.mips);
            }
        }
        
        if (fastestMIPS == 0) {
            // System.out.println("Warning: No eligible VMs found for job " + job.id);
            continue;
        }
        
        double runtime = job.mi / fastestMIPS;
        double singleTaskReliability = getReliability(runtime, lambda);
        double taskReliability = 1.0 - Math.pow(1.0 - singleTaskReliability, replicaCounts[i]);
        
        systemReliability *= taskReliability;
    }
    
    return systemReliability;
}

// public static double[] optimizeReplicaCounts(List<Job> jobs, List<VMData> vms, double lambda, double deadline, double requiredReliability) {
//     int n = jobs.size();
//     double[] x = new double[n];
    
//     // Better initialization: estimate minimum replicas needed
//     for (int i = 0; i < n; i++) {
//         Job job = jobs.get(i);
//         double runtime = job.mi / getFastestMIPS(vms);
//         double singleTaskReliability = getReliability(runtime, lambda);
        
//         // Calculate minimum replicas needed for individual task reliability
//         // For system reliability R_sys = product R_i, we need each R_i ≥ R_sys^(1/n)
//         double targetTaskReliability = Math.pow(requiredReliability, 1.0 / n);
        
//         if (singleTaskReliability >= targetTaskReliability) {
//             x[i] = 1.0;
//         } else {
//             // Solve: 1 - (1-p)^k = targetTaskReliability
//             // k = ln(1 - targetTaskReliability) / ln(1 - p)
//             double failureRate = 1.0 - singleTaskReliability;
//             if (failureRate > 0 && failureRate < 1) {
//                 x[i] = Math.max(1.0, Math.ceil(Math.log(1.0 - targetTaskReliability) / Math.log(failureRate)));
//             } else {
//                 x[i] = 1.0;
//             }
//         }
//     }
    
//     System.out.println("Initial replica counts based on target reliability:");
//     for (int i = 0; i < n; i++) {
//         System.out.printf("Job %s: %.2f replicas\n", jobs.get(i).id, x[i]);
//     }
    
//     // Check if initial solution meets requirement
//     double initialReliability = calculateSystemReliability(x, jobs, lambda, vms);
//     System.out.printf("Initial system reliability: %.6f (required: %.6f)\n", initialReliability, requiredReliability);
    
//     if (initialReliability >= requiredReliability) {
//         System.out.println("Initial solution already meets reliability requirement");
//         return x;
//     }
    
//     // If not, use iterative improvement
//     double mu = 1.0;
//     double learningRate = 0.05;
//     double muLearningRate = 0.1;
//     int maxIters = 1000;
    
//     // Pre-calculate VM characteristics
//     double[] cheapestCosts = new double[n];
//     double[] fastestMIPS = new double[n];
    
//     for (int i = 0; i < n; i++) {
//         Job job = jobs.get(i);
//         double minCost = Double.MAX_VALUE;
//         double maxMIPS = 0;
        
//         for (VMData vm : vms) {
//             double runtime = job.mi / vm.mips;
//             if (runtime <= job.subDeadline) {
//                 minCost = Math.min(minCost, vm.cost);
//                 maxMIPS = Math.max(maxMIPS, vm.mips);
//             }
//         }
        
//         cheapestCosts[i] = minCost == Double.MAX_VALUE ? getCheapestCost(vms) : minCost;
//         fastestMIPS[i] = maxMIPS == 0 ? getFastestMIPS(vms) : maxMIPS;
//     }
    
//     for (int iter = 0; iter < maxIters; iter++) {
//         // Calculate current system reliability
//         double systemReliability = 1.0;
//         double totalCost = 0.0;
        
//         for (int i = 0; i < n; i++) {
//             Job job = jobs.get(i);
//             double runtime = job.mi / fastestMIPS[i];
//             double singleTaskReliability = getReliability(runtime, lambda);
//             double taskReliability = 1.0 - Math.pow(1.0 - singleTaskReliability, x[i]);
            
//             systemReliability *= taskReliability;
//             totalCost += cheapestCosts[i] * runtime * x[i];
//         }
        
//         // Constraint: systemReliability >= requiredReliability
//         double constraintViolation = requiredReliability - systemReliability;
        
//         // Only update if constraint is violated
//         if (constraintViolation > 1e-6) {
//             // Update replica counts
//             for (int i = 0; i < n; i++) {
//                 Job job = jobs.get(i);
//                 double runtime = job.mi / fastestMIPS[i];
//                 double singleTaskReliability = getReliability(runtime, lambda);
//                 double failureRate = 1.0 - singleTaskReliability;
                
//                 if (failureRate > 1e-10 && failureRate < 1.0) {
//                     // Cost gradient
//                     double costGradient = cheapestCosts[i] * runtime;
                    
//                     // Reliability gradient: d(systemReliability)/dx[i]
//                     double currentTaskReliability = 1.0 - Math.pow(failureRate, x[i]);
//                     double reliabilityGradient = 0.0;
                    
//                     if (currentTaskReliability > 1e-10) {
//                         reliabilityGradient = systemReliability * Math.pow(failureRate, x[i]) * Math.log(failureRate) / currentTaskReliability;
//                     }
                    
//                     // Update with penalty method
//                     double penalty = 1000.0 * constraintViolation;
//                     double totalGradient = costGradient - penalty * reliabilityGradient;
                    
//                     x[i] -= learningRate * totalGradient;
//                     x[i] = Math.max(1.0, Math.min(10.0, x[i]));
//                 }
//             }
            
//             // Update penalty multiplier
//             mu = Math.max(0.0, mu + muLearningRate * constraintViolation);
//         } else {
//             // Constraint satisfied, try to minimize cost
//             break;
//         }
        
//         // Print progress
//         if (iter % 100 == 0) {
//             System.out.printf("Iter %d: Cost=%.2f, Reliability=%.6f, Violation=%.6f\n", 
//                             iter, totalCost, systemReliability, constraintViolation);
//         }
        
//         // Check convergence
//         if (Math.abs(constraintViolation) < 1e-6) {
//             System.out.println("Converged at iteration " + iter);
//             break;
//         }
//     }
    
//     // Final verification
//     double finalReliability = calculateSystemReliability(x, jobs, lambda, vms);
//     System.out.printf("Final system reliability: %.6f (required: %.6f)\n", finalReliability, requiredReliability);
    
//     return x;
// }

// // Helper method to calculate system reliability
// public static double calculateSystemReliability(double[] replicaCounts, List<Job> jobs, double lambda, List<VMData> vms) {
//     double systemReliability = 1.0;
    
//     for (int i = 0; i < jobs.size(); i++) {
//         Job job = jobs.get(i);
//         double runtime = job.mi / getFastestMIPS(vms);
//         double singleTaskReliability = getReliability(runtime, lambda);
//         double taskReliability = 1.0 - Math.pow(1.0 - singleTaskReliability, replicaCounts[i]);
        
//         systemReliability *= taskReliability;
//     }
    
//     return systemReliability;
// }

// Also add this debugging method to see what's happening
public static void debugReliabilityCalculation(List<Job> jobs, List<VMData> vms, double lambda, double requiredReliability) {
    System.out.println("=== Debugging Reliability Calculation ===");
    System.out.printf("Lambda: %.3f, Required Reliability: %.6f\n", lambda, requiredReliability);
    
    for (int i = 0; i < jobs.size(); i++) {
        Job job = jobs.get(i);
        double runtime = job.mi / getFastestMIPS(vms);
        double singleTaskReliability = getReliability(runtime, lambda);
        
        System.out.printf("Job %s: runtime=%.2f, single_reliability=%.6f\n", 
                         job.id, runtime, singleTaskReliability);
    }
    
    // Calculate what system reliability would be with all tasks having 1 replica
    double systemReliabilityWith1Replica = 1.0;
    for (Job job : jobs) {
        double runtime = job.mi / getFastestMIPS(vms);
        double singleTaskReliability = getReliability(runtime, lambda);
        systemReliabilityWith1Replica *= singleTaskReliability;
    }
    
    System.out.printf("System reliability with 1 replica each: %.6f\n", systemReliabilityWith1Replica);
    System.out.printf("Gap to required reliability: %.6f\n", requiredReliability - systemReliabilityWith1Replica);
}

// public static double[] optimizeReplicaCountsWorkflowAware(List<Job> jobs, List<VMData> vms, double lambda, double deadline, double requiredReliability, String workflowName) {
//     int n = jobs.size();
//     double[] x = new double[n];
    
//     // First, analyze the workflow
//     analyzeWorkflowCharacteristics(jobs, vms, lambda, requiredReliability, workflowName);
    
//     // Workflow-specific lambda adjustment
//     double adjustedLambda = lambda;
//     if (workflowName.toLowerCase().contains("sipht")) {
//         adjustedLambda = lambda * 2.0; // Sipht might have different failure characteristics
//         System.out.println("Adjusted lambda for Sipht workflow: " + adjustedLambda);
//     } else if (workflowName.toLowerCase().contains("montage")) {
//         adjustedLambda = lambda * 1.5; // Montage might have different failure characteristics
//         System.out.println("Adjusted lambda for Montage workflow: " + adjustedLambda);
//     }
    
//     // Initialize based on workflow characteristics
//     double fastestMIPS = getFastestMIPS(vms);
    
//     // Calculate system reliability with single replicas first
//     double systemReliabilityWith1 = 1.0;
//     for (Job job : jobs) {
//         double runtime = job.mi / fastestMIPS;
//         double singleTaskReliability = getReliability(runtime, adjustedLambda);
//         systemReliabilityWith1 *= singleTaskReliability;
//     }
    
//     System.out.printf("System reliability with 1 replica each: %.6f\n", systemReliabilityWith1);
    
//     if (systemReliabilityWith1 >= requiredReliability) {
//         System.out.println("Single replicas sufficient for " + workflowName);
//         Arrays.fill(x, 1.0);
//         return x;
//     }
    
//     // Calculate how much improvement we need
//     double reliabilityGap = requiredReliability - systemReliabilityWith1;
//     System.out.printf("Reliability gap to fill: %.6f\n", reliabilityGap);
    
//     // Initialize with better estimates
//     for (int i = 0; i < n; i++) {
//         Job job = jobs.get(i);
//         double runtime = job.mi / fastestMIPS;
//         double singleTaskReliability = getReliability(runtime, adjustedLambda);
        
//         // For workflows with many tasks, we need higher per-task reliability
//         double targetTaskReliability = Math.pow(requiredReliability, 1.0 / n);
        
//         if (singleTaskReliability >= targetTaskReliability) {
//             x[i] = 1.0;
//         } else {
//             double failureRate = 1.0 - singleTaskReliability;
//             if (failureRate > 0 && failureRate < 1) {
//                 double minReplicas = Math.log(1.0 - targetTaskReliability) / Math.log(failureRate);
//                 x[i] = Math.max(1.0, Math.ceil(minReplicas));
                
//                 // Cap replicas for large workflows
//                 if (n > 50) {
//                     x[i] = Math.min(x[i], 5.0); // Max 5 replicas for large workflows
//                 } else if (n > 20) {
//                     x[i] = Math.min(x[i], 8.0); // Max 8 replicas for medium workflows
//                 }
//             } else {
//                 x[i] = 1.0;
//             }
//         }
//     }
    
//     // Check if this initial solution works
//     double initialReliability = calculateSystemReliabilityWithLambda(x, jobs, adjustedLambda, vms);
//     System.out.printf("Initial system reliability: %.6f\n", initialReliability);
    
//     if (initialReliability >= requiredReliability) {
//         System.out.println("Initial solution meets requirement for " + workflowName);
//         return x;
//     }
    
//     // If still not enough, use iterative optimization
//     return optimizeWithIterativeRefinement(x, jobs, vms, adjustedLambda, requiredReliability, workflowName);
// }

// // Helper method for iterative refinement
// private static double[] optimizeWithIterativeRefinement(double[] x, List<Job> jobs, List<VMData> vms, double lambda, double requiredReliability, String workflowName) {
//     int n = jobs.size();
//     int maxIters = 500;
//     double tolerance = 1e-6;
    
//     for (int iter = 0; iter < maxIters; iter++) {
//         double currentReliability = calculateSystemReliabilityWithLambda(x, jobs, lambda, vms);
        
//         if (currentReliability >= requiredReliability - tolerance) {
//             System.out.printf("Converged at iteration %d for %s\n", iter, workflowName);
//             break;
//         }
        
//         // Find the task with lowest reliability and increase its replicas
//         int worstTaskIndex = -1;
//         double lowestReliability = Double.MAX_VALUE;
        
//         for (int i = 0; i < n; i++) {
//             Job job = jobs.get(i);
//             double runtime = job.mi / getFastestMIPS(vms);
//             double singleTaskReliability = getReliability(runtime, lambda);
//             double taskReliability = 1.0 - Math.pow(1.0 - singleTaskReliability, x[i]);
            
//             if (taskReliability < lowestReliability && x[i] < 10.0) {
//                 lowestReliability = taskReliability;
//                 worstTaskIndex = i;
//             }
//         }
        
//         if (worstTaskIndex != -1) {
//             x[worstTaskIndex] += 1.0;
//         } else {
//             // If all tasks are at max replicas, break
//             break;
//         }
        
//         if (iter % 50 == 0) {
//             System.out.printf("Iter %d: Reliability=%.6f, Gap=%.6f\n", 
//                             iter, currentReliability, requiredReliability - currentReliability);
//         }
//     }
    
//     return x;
// }

// // Helper method to calculate system reliability with specific lambda
// public static double calculateSystemReliabilityWithLambda(double[] replicaCounts, List<Job> jobs, double lambda, List<VMData> vms) {
//     double systemReliability = 1.0;
    
//     for (int i = 0; i < jobs.size(); i++) {
//         Job job = jobs.get(i);
//         double runtime = job.mi / getFastestMIPS(vms);
//         double singleTaskReliability = getReliability(runtime, lambda);
//         double taskReliability = 1.0 - Math.pow(1.0 - singleTaskReliability, replicaCounts[i]);
        
//         systemReliability *= taskReliability;
//     }
    
//     return systemReliability;
// }

// // Replace your current optimizeReplicaCounts call with this:
// public static double[] optimizeReplicaCounts(List<Job> jobs, List<VMData> vms, double lambda, double deadline, double requiredReliability) {
//     // Extract workflow name from the current file being processed
//     String workflowName = "Unknown";
//     // You can pass this as a parameter or determine it from the jobs
    
//     return optimizeReplicaCountsWorkflowAware(jobs, vms, lambda, deadline, requiredReliability, workflowName);
// }
private static double getFastestMIPS(List<VMData> vms) {
    return vms.stream().mapToDouble(vm -> vm.mips).max().orElse(1000);
}

private static double getCheapestCost(List<VMData> vms) {
    return vms.stream().mapToDouble(vm -> vm.cost).min().orElse(0.01);
}

public static void analyzeWorkflowCharacteristics(List<Job> jobs, List<VMData> vms, double lambda, double requiredReliability, String workflowName) {
    System.out.println("=== Analyzing Workflow: " + workflowName + " ===");
    System.out.printf("Lambda: %.3f, Required Reliability: %.6f\n", lambda, requiredReliability);
    System.out.printf("Number of tasks: %d\n", jobs.size());
    
    // Analyze task characteristics
    double totalMI = 0, minMI = Double.MAX_VALUE, maxMI = 0;
    double totalRuntime = 0, minRuntime = Double.MAX_VALUE, maxRuntime = 0;
    
    for (Job job : jobs) {
        totalMI += job.mi;
        minMI = Math.min(minMI, job.mi);
        maxMI = Math.max(maxMI, job.mi);
        
        totalRuntime += job.runtime;
        minRuntime = Math.min(minRuntime, job.runtime);
        maxRuntime = Math.max(maxRuntime, job.runtime);
    }
    
    System.out.printf("MI Stats: min=%.2f, max=%.2f, avg=%.2f\n", minMI, maxMI, totalMI/jobs.size());
    System.out.printf("Runtime Stats: min=%.2f, max=%.2f, avg=%.2f\n", minRuntime, maxRuntime, totalRuntime/jobs.size());
    
    // Analyze VM characteristics
    double fastestMIPS = getFastestMIPS(vms);
    double slowestMIPS = vms.stream().mapToDouble(vm -> vm.mips).min().orElse(1000);
    System.out.printf("VM MIPS range: %.2f to %.2f\n", slowestMIPS, fastestMIPS);
    
    // Calculate reliability for each task with fastest VM
    double systemReliabilityFastest = 1.0;
    double systemReliabilitySlowest = 1.0;
    
    System.out.println("\nTask-by-task reliability analysis:");
    for (int i = 0; i < Math.min(10, jobs.size()); i++) { // Show first 10 tasks
        Job job = jobs.get(i);
        double runtimeFastest = job.mi / fastestMIPS;
        double runtimeSlowest = job.mi / slowestMIPS;
        
        double reliabilityFastest = getReliability(runtimeFastest, lambda);
        double reliabilitySlowest = getReliability(runtimeSlowest, lambda);
        
        systemReliabilityFastest *= reliabilityFastest;
        systemReliabilitySlowest *= reliabilitySlowest;
        
        System.out.printf("Task %s: runtime_fast=%.2f, rel_fast=%.6f, runtime_slow=%.2f, rel_slow=%.6f\n",
                         job.id, runtimeFastest, reliabilityFastest, runtimeSlowest, reliabilitySlowest);
    }
    
    System.out.printf("System reliability (fastest VMs): %.6f\n", systemReliabilityFastest);
    System.out.printf("System reliability (slowest VMs): %.6f\n", systemReliabilitySlowest);
    
    // Calculate theoretical minimum replicas needed
    double targetTaskReliability = Math.pow(requiredReliability, 1.0 / jobs.size());
    System.out.printf("Target per-task reliability: %.6f\n", targetTaskReliability);
    
    int tasksNeedingReplicas = 0;
    for (Job job : jobs) {
        double runtime = job.mi / fastestMIPS;
        double singleTaskReliability = getReliability(runtime, lambda);
        if (singleTaskReliability < targetTaskReliability) {
            tasksNeedingReplicas++;
        }
    }
    
    System.out.printf("Tasks needing replicas: %d out of %d\n", tasksNeedingReplicas, jobs.size());
    System.out.println("==========================================\n");
}

public static void getEligibleVMs(List<Job> jobs, List<VMData> vms, Map<String, Double> fileSizeMap) {
    

    for (Job job : jobs) {
        List<VMData>eligibleVMs=new ArrayList<>();
        List<String> outputFiles = job.outputFiles;
            List<String> inputFiles = job.inputFiles;
            double reqdStorage = 0.0;

            for (String file : outputFiles) {
                if(fileSizeMap.containsKey(file))
                reqdStorage += fileSizeMap.get(file);
            }
            for (String file : inputFiles) {
                 if(fileSizeMap.containsKey(file))
                reqdStorage += fileSizeMap.get(file);
            }
          
        for (VMData vm : vms) {
            double runtime = job.mi / vm.mips;
            double storage = vm.storage;

            

            if (runtime < job.subDeadline && (reqdStorage / 1024) <= storage*1024) {
                eligibleVMs.add(vm);
                
            }
        }
        job.eligibleVMS=eligibleVMs;
    }

    
}



}
