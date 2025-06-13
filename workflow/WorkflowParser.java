import javax.xml.parsers.*;
import org.w3c.dom.*;
import java.io.File;
import java.util.*;

public class WorkflowParser {
    static class Job {
    String id;
    double runtime;
    double mi;
     double subDeadline;
    List<String> children = new ArrayList<>();
    List<String> parents = new ArrayList<>();
     double startTime;
    double endTime;
    VMData assignedVM;

    public Job(String id, double runtime, double mi) {
        this.id = id;
        this.runtime = runtime;
        this.mi=mi;
        subDeadline=0.0;

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

    public static void main(String[] args) {
        Scanner sc=new Scanner(System.in);
        int userDeadline=sc.nextInt();
        try {
            File xmlFile = new File("Montage_50.xml"); // Replace with your file
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(xmlFile);

            doc.getDocumentElement().normalize();

            Map<String, Job> taskMap = new HashMap<>();

            // Extract all jobs
            NodeList jobList = doc.getElementsByTagNameNS("*", "job");
            Set<String> jobIds = new HashSet<>();
            for (int i = 0; i < jobList.getLength(); i++) {
                Element job = (Element) jobList.item(i);
                jobIds.add(job.getAttribute("id"));
                String jobId = job.getAttribute("id");
                 double runtime = Double.parseDouble(job.getAttribute("runtime"));
                 double mi=0;
                 switch (job.getAttribute("name")) {
    case "mProject": mi = 5000; break;
    case "mDiffFit": mi = 8000; break;
    case "mConcatFit": mi = 2000; break;
    case "mBgModel": mi = 3000; break;
    default: mi = 40000;
}

                Job newJob = new Job(jobId, runtime,mi);
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

//             for (Map.Entry<String, Job> entry : taskMap.entrySet()) {
//     Job task = entry.getValue();
//     System.out.println(task); // or task.getId(), task.getMi(), etc.
// }


            List<String>sortedJobs = topologicalSort(taskMap);

           Map<String,Double>UpwardRanks= calculateUpwardRanks(sortedJobs, taskMap);
            double entryRank=0.0;
           for(Map.Entry<String,Double>entry:UpwardRanks.entrySet()){
            double rank=entry.getValue();
            entryRank=Math.max(rank,entryRank);
           
           }
           System.out.println("entryRank="+entryRank);


           for( Map.Entry<String,Job>entry:taskMap.entrySet()){
            Job current=entry.getValue();
            current.subDeadline = (UpwardRanks.get(current.id) / entryRank) * userDeadline;

           }

           List<Job>UpwardRankSortedJobs=sortUpwardRanks(sortedJobs, UpwardRanks, taskMap);
           HashMap<VMData,Double>vmAvail=new HashMap<>();
           List<VMData>vms=VMData.parseCSV("cleaned_vm_data.csv"); 
            for(VMData vm:vms){
                vmAvail.put(vm,0.0);
            }
            List<Job>scheduledjob=scheduledJobs( UpwardRankSortedJobs,vms,
                                     vmAvail, taskMap);

            for (Job scheduled : scheduledjob) {
    System.out.println("Task ID: " + scheduled.id);
    System.out.println("Parents: " + scheduled.parents);
    System.out.println("Start Time: " + scheduled.startTime);
    System.out.println("End Time: " + scheduled.endTime);
    System.out.println("Assigned VM: " + (scheduled.assignedVM != null ? scheduled.assignedVM.id : "None"));
    System.out.println("-----------");
}

            
           

           
           

            

        } catch (Exception e) {
            e.printStackTrace();
        }
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

    public static List<Job> scheduledJobs(List<Job> UpwardRankSortedJobs, List<VMData> vms,
                                      HashMap<VMData, Double> vmAvailability, Map<String, Job> taskMap) {
    List<Job> schedule = new ArrayList<>();
    double totalCost = 0.0;

    for (Job task : UpwardRankSortedJobs) {
        double minCost = Double.MAX_VALUE;
        VMData bestVM = null;
        double bestEST = 0.0;
        double bestEFT = 0.0;

        for (VMData vm : vms) {
            double avail = vmAvailability.getOrDefault(vm, 0.0);
            double est = avail;

            // Calculate EST based on parent dependencies
            for (String parentId : task.parents) {
                Job parentJob = taskMap.get(parentId);
                if (parentJob == null || parentJob.endTime == 0.0) continue;

                double parentEnd = parentJob.endTime;
                double commTime = (parentJob.assignedVM == vm) ? 0.0 : 1.0;
                est = Math.max(est, parentEnd + commTime);
            }

            double runtime = task.mi / vm.mips;
            double eft = est + runtime;

            if (eft <= task.subDeadline) {
                double cost = (runtime / 3600.0) * vm.cost;
                if (cost < minCost) {
                    minCost = cost;
                    bestVM = vm;
                    bestEST = est;
                    bestEFT = eft;
                }
            }
        }

        // Final assignment if any valid VM was found
        if (bestVM != null) {
            task.assignedVM = bestVM;
            task.startTime = bestEST;
            task.endTime = bestEFT;
            vmAvailability.put(bestVM, bestEFT);
            totalCost += minCost;
            schedule.add(task);
        } else {
            System.err.println("Deadline constraint cannot be met for task: " + task.id);
        }
    }

  System.out.println("Total optimized cost: $" + String.format("%.6f", totalCost));

    return schedule;
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



}
