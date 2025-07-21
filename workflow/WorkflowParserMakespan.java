// This is a modified version of your WorkflowParser class
// The scheduling algorithm now operates under a fixed budget constraint

import javax.xml.parsers.*;
import org.w3c.dom.*;
import java.io.File;
import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class WorkflowParserMakespan {
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
        try {
            List<Double> budgets = Arrays.asList(  1.0,
    1.1,
    1.21,
    1.33,
    1.46,
    1.61,
    1.77,
    1.95,
    2.14,
    2.36,
    2.59,
    2.85,
    3.14,
    3.45,
    3.79,
    4.17,
    4.58,
    5.04,
    5.54,
    6.09,
    6.7,
    7.37,
    8.11,
    8.92,
    9.81,
    10.79,
    11.87,
    13.06,
    14.37,
    15.81,
    17.39,
    19.13);
             List<String> files=Arrays.asList("CyberShake_30.xml", "Epigenomics_46.xml", "Inspiral_30.xml", "Inspiral_50.xml", "Inspiral_30.xml",
            "Montage_50.xml","Sipht_30.xml");
            List<String[]> rows = new ArrayList<>();

            for (String file : files) {
                File xmlFile = new File(file);
                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                factory.setNamespaceAware(true);
                DocumentBuilder builder = factory.newDocumentBuilder();
                Document doc = builder.parse(xmlFile);
                doc.getDocumentElement().normalize();

                Map<String, Job> taskMap = new HashMap<>();
                Map<String, String> fileToProducerTaskMap = new HashMap<>();
                Map<String, Double> fileSizeMap = new HashMap<>();
                List<VMData> vms = VMData.parseCSV("cleaned_vm_data.csv");
                double median_mips = getMedianMIPS(vms);
                double median_bw = getMedianBW(vms);

                NodeList jobList = doc.getElementsByTagNameNS("*", "job");
                for (int i = 0; i < jobList.getLength(); i++) {
                    Element job = (Element) jobList.item(i);
                    String jobId = job.getAttribute("id");
                    double runtime = Double.parseDouble(job.getAttribute("runtime"));
                    double mi = runtime * median_mips;

                    NodeList usesList = job.getElementsByTagName("uses");
                    List<String> inputFiles = new ArrayList<>();
                    List<String> outputFiles = new ArrayList<>();
                    for (int j = 0; j < usesList.getLength(); j++) {
                        Element usesElement = (Element) usesList.item(j);
                        String fileName = usesElement.getAttribute("file");
                        String linkType = usesElement.getAttribute("link");
                        double size = usesElement.hasAttribute("size") ? Double.parseDouble(usesElement.getAttribute("size")) : 0.0;
                        if (linkType.equalsIgnoreCase("output")) {
                            fileToProducerTaskMap.put(fileName, jobId);
                            fileSizeMap.put(fileName, size);
                            outputFiles.add(fileName);
                        } else if (linkType.equalsIgnoreCase("input")) {
                            inputFiles.add(fileName);
                        }
                    }

                    Job newJob = new Job(jobId, runtime, mi, inputFiles, outputFiles);
                    taskMap.put(jobId, newJob);
                }

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

                List<String> sortedJobs = topologicalSort(taskMap);
                computeEST_EFT(sortedJobs, taskMap, vms, median_bw, fileSizeMap, fileToProducerTaskMap);

                for (double budget : budgets) {
                    HashMap<VMData, Double> vmAvail = new HashMap<>();
                    for (VMData vm : vms) vmAvail.put(vm, 0.0);

                    List<Job> copyJobs = deepCopyJobs(taskMap);
                    ScheduleResult result = scheduleWithFixedBudget(copyJobs, vms, budget, vmAvail, taskMap, fileSizeMap, fileToProducerTaskMap);
                    double makespan = result.schedule.stream().mapToDouble(j -> j.endTime).max().orElse(-1);
                    rows.add(new String[]{file, String.valueOf(budget), String.valueOf(makespan)});
                }
            }
            saveBudgetVsMakespan("budget_vs_makespan_V1.csv", rows);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

     public static void saveBudgetVsMakespan(String fileName, List<String[]> rows) {
    try (PrintWriter writer = new PrintWriter(new File(fileName))) {
        writer.println("Workflow,Cost,Makespan"); // header
        for (String[] row : rows) {
            writer.println(String.join(",", row));
        }
        System.out.println("CSV written to: " + fileName);
    } catch (FileNotFoundException e) {
        System.err.println("Error writing CSV: " + e.getMessage());
    }
}
public static ScheduleResult scheduleWithFixedBudget(List<Job> jobs, List<VMData> vms, double maxAllowedCost,
    HashMap<VMData, Double> vmAvailability, Map<String, Job> taskMap,
    Map<String, Double> fileSizeMap, Map<String, String> fileToProducerTaskMap) {

    List<Job> schedule = new ArrayList<>();
    Map<VMData, List<Job>> vmJobMap = new HashMap<>();
    for (VMData vm : vms) vmJobMap.put(vm, new ArrayList<>());

    double totalCost = 0.0;

    // Sort jobs by upward rank descending (most critical first)
    List<Job> sortedJobs = new ArrayList<>(jobs);
    sortedJobs.sort((a, b) -> Double.compare(b.subDeadline, a.subDeadline)); // or upward rank if available

    for (Job task : sortedJobs) {
        VMData bestVM = null;
        double bestEST = 0.0;
        double bestEFT = Double.MAX_VALUE;
        double bestCost = 0.0;
        double bestScore = Double.MAX_VALUE;

        for (VMData vm : vms) {
            double est = vmAvailability.getOrDefault(vm, 0.0);
            double dataReadyTime = 0.0;

            for (String parentId : task.parents) {
                Job parent = taskMap.get(parentId);
                if (parent == null) continue;
                double commTime = 0.0;
                if (parent.assignedVM != vm) {
                    double size = getDataSizeTransferred(parent, task, fileSizeMap, fileToProducerTaskMap);
                    commTime = size / (vm.networkPerformance * 125.0);
                }
                est = Math.max(est, parent.endTime + commTime);
                dataReadyTime = Math.max(dataReadyTime, parent.endTime + commTime);
            }

            est = Math.max(est, dataReadyTime);
            double runtime = task.mi / vm.mips;
            double eft = est + runtime;
            double cost = (runtime / 3600.0) * vm.cost;

            // Score = normalized eft + normalized cost
            double normalizedCost = cost / maxAllowedCost;
            double normalizedEFT = eft / 1e5; // Normalize based on max expected makespan
            double score = 0.6 * normalizedEFT + 0.4 * normalizedCost;

            if ((totalCost + cost <= maxAllowedCost) && score < bestScore) {
                bestVM = vm;
                bestEST = est;
                bestEFT = eft;
                bestCost = cost;
                bestScore = score;
            }
        }

        if (bestVM != null) {
            task.assignedVM = bestVM;
            task.startTime = bestEST;
            task.endTime = bestEFT;

            totalCost += bestCost;
            vmAvailability.put(bestVM, bestEFT);
            vmJobMap.get(bestVM).add(task);
            schedule.add(task);
        } else {
            System.err.println("WARNING: Could not schedule task " + task.id + " within budget.");
        }
    }

    return new ScheduleResult(schedule, totalCost);
}


    public static List<Job> deepCopyJobs(Map<String, Job> taskMap) {
        List<Job> copy = new ArrayList<>();
        for (Job j : taskMap.values()) {
            copy.add(new Job(j.id, j.runtime, j.mi, j.inputFiles, j.outputFiles));
        }
        return copy;
    }

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

    // Placeholder for , , , ,
    // , and saveBudgetVsMakespan methods — reuse your existing definitions.
}
