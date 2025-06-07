import javax.xml.parsers.*;
import org.w3c.dom.*;
import java.io.File;
import java.util.*;

public class WorkflowParser {
    static class Job {
    String id;
    double runtime;
    List<String> children = new ArrayList<>();
    List<String> parents = new ArrayList<>();

    public Job(String id, double runtime) {
        this.id = id;
        this.runtime = runtime;
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
                Job newJob = new Job(jobId, runtime);
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

            List<VM>vms=VM.getDefaultVMs(5); // Create 5 VMs
            List<ScheduledTask> schedule = scheduleJobs(sortedJobs, taskMap, vms);
            // // Output the schedule job-wise
            // for (ScheduledTask task : schedule) {
            //     System.out.println("Job ID: " + task.jobId + ", VM ID: " + task.vmId + 
            //                        ",  Time: " + task.startTime + "----> " + task.endTime);
            // }
            //output the schedule VM-wise
            HashMap<String,List<ScheduledJob>>VMwiseJobs=new HashMap<>();
          
            for(ScheduledTask task: schedule){
                if(!VMwiseJobs.containsKey(task.vmId)){
                    VMwiseJobs.put(task.vmId, new ArrayList<>());
                }
                VMwiseJobs.get(task.vmId).add(new ScheduledJob(task.jobId, task.startTime, task.endTime));
            }

            for (Map.Entry<String, List<ScheduledJob>> entry : VMwiseJobs.entrySet()) {
                String vmId = entry.getKey();
                List<ScheduledJob> jobs = entry.getValue();
                System.out.println("VM ID: " + vmId);
                for (ScheduledJob job : jobs) {
                    System.out.println("  Job ID: " + job.jobId.substring(5) + ", Time: " + job.startTime + "----> " + job.endTime+"\n"+"Parents ="+ taskMap.get(job.jobId).parents);
                }
            }

           
           

            

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //writing a function to convert it to topological sort
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

    //scheduler function
    public static List<ScheduledTask> scheduleJobs(List<String> sortedJobs, Map<String, Job> taskMap, List<VM> vms) {
    List<ScheduledTask> schedule = new ArrayList<>();
    Map<String, ScheduledTask> taskExecution = new HashMap<>();

    for (String jobId : sortedJobs) {
        Job job = taskMap.get(jobId);

        double bestEFT = Double.MAX_VALUE;
        VM bestVM = null;
        double bestStartTime = 0;

        for (VM vm : vms) {
            // Compute Earliest Start Time (EST)
            double est = vm.availableAt;

            for (String parentId : job.parents) {
                ScheduledTask parentTask = taskExecution.get(parentId);
                double commTime = parentTask.vmId.equals(vm.id) ? 0.0 : 1.0;
                est = Math.max(est, parentTask.endTime + commTime);
            }

            double eft = est + job.runtime;

            if (eft < bestEFT) {
                bestEFT = eft;
                bestStartTime = est;
                bestVM = vm;
            }
        }

        // Assign task to best VM
        ScheduledTask scheduled = new ScheduledTask(jobId, bestVM.id, bestStartTime, bestEFT);
        taskExecution.put(jobId, scheduled);
        schedule.add(scheduled);

        // Update VM availability
        bestVM.availableAt = bestEFT;
    }

    return schedule;
}



}
