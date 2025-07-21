import javax.xml.parsers.*;
import org.w3c.dom.*;
import java.io.File;
import java.io.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CPRS {
    static final double REQUIRED_RELIABILITY = 0.99;
    static final double LAMBDA = 0.001;
    static final int BILLING_UNIT = 3600; // seconds

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
        List<String> inputFiles;
        List<String> outputFiles;

        public Job(String id, double runtime, double mi, List<String> inputFiles, List<String> outputFiles) {
            this.id = id;
            this.runtime = runtime;
            this.mi = mi;
            this.subDeadline = 0.0;
            this.slack = 0.0;
            this.inputFiles = inputFiles;
            this.outputFiles = outputFiles;
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

        public ScheduledJob(String jobId, double startTime, double endTime) {
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
           
             List<String> files=Arrays.asList("CyberShake_30.xml", "Epigenomics_46.xml", "Inspiral_30.xml", "Inspiral_50.xml", "Inspiral_30.xml",
            "Montage_50.xml");
            double dfactors[]={1.0};
            double reqrel[]={0.90,0.95,0.99};
            List<String[]> rows = new ArrayList<>();
            // ----- Load VMs -----
            List<VMData> vms = VMData.parseCSV("filtered_vm_data.csv");
             for(String file:files)
           { 
              // ----- Parse Workflow XML -----
            //String filePath = "Inspiral_50.xml"; // Provide path to workflow XML
            File xmlFile = new File(file);
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(xmlFile);
            doc.getDocumentElement().normalize();

            Map<String, Job> taskMap = new HashMap<>();
            NodeList jobListXML = doc.getElementsByTagNameNS("*", "job");

            double medianMIPS = vms.stream()
                    .map(vm -> vm.mips)
                    .sorted()
                    .skip(vms.size() / 2)
                    .findFirst()
                    .orElse(1000.0);

            for (int i = 0; i < jobListXML.getLength(); i++) {
                Element jobElement = (Element) jobListXML.item(i);
                String jobId = jobElement.getAttribute("id");
                double runtime = Double.parseDouble(jobElement.getAttribute("runtime"));
                double mi = runtime * medianMIPS;

                NodeList usesList = jobElement.getElementsByTagName("uses");
                List<String> inputFiles = new ArrayList<>();
                List<String> outputFiles = new ArrayList<>();
                for (int j = 0; j < usesList.getLength(); j++) {
                    Element usesElement = (Element) usesList.item(j);
                    String fileName = usesElement.getAttribute("file");
                    String linkType = usesElement.getAttribute("link");
                    if (linkType.equalsIgnoreCase("input")) inputFiles.add(fileName);
                    if (linkType.equalsIgnoreCase("output")) outputFiles.add(fileName);
                }

                Job job = new Job(jobId, runtime, mi, inputFiles, outputFiles);
                taskMap.put(jobId, job);
            }

            NodeList childList = doc.getElementsByTagNameNS("*", "child");
            for (int i = 0; i < childList.getLength(); i++) {
                Element child = (Element) childList.item(i);
                String childId = child.getAttribute("ref");
                NodeList parentNodes = child.getElementsByTagNameNS("*", "parent");
                for (int j = 0; j < parentNodes.getLength(); j++) {
                    Element parent = (Element) parentNodes.item(j);
                    String parentId = parent.getAttribute("ref");
                    taskMap.get(parentId).children.add(childId);
                    taskMap.get(childId).parents.add(parentId);
                }
            }

            List<Job> jobList = new ArrayList<>(taskMap.values());
             List<String> reverseTopological = getReverseTopological(jobList, taskMap);
            Map<String, double[]> OC = computeOC(jobList, vms, taskMap,reverseTopological);
        Map<String, Double> rank = rankByOC(OC, vms);
        List<Job> rankedJobs = sortByRank(jobList, rank);
         double entryRank=0.0;
           for(Map.Entry<String,Double>entry:rank.entrySet()){
            double temprank=entry.getValue();
            entryRank=Math.max(temprank,entryRank);
           
           }
            for(double df:dfactors)
           {
            for(double rel:reqrel){
        //    double userDeadline=df*entryRank;
            // ----- Schedule Jobs -----
            double deadline = entryRank*df; // You can change this
            ScheduleResult result = schedule(jobList, vms, taskMap, deadline,  OC ,
         rank ,
        rankedJobs,rel);

            // ----- Print Output -----
            System.out.println("\n--- Scheduled Jobs ---");
            for (Job job : result.schedule) {
                System.out.printf("Job %-4s | VM %-4s | Start: %6.2f | End: %6.2f\n",
                        job.id,
                        (job.assignedVM != null ? job.assignedVM.id : "None"),
                        job.startTime,
                        job.endTime);
            }
            System.out.printf("\nTotal Cost: $%.4f\n", result.totalCost);
             rows.add(new String[]{file, String.valueOf(rel), String.valueOf(result.totalCost)});
           }
           }
             saveDeadlinesWorkflows("CPRS-wise-reliability-99-costs.csv",rows);
        }} catch (Exception e) {
            e.printStackTrace();
        }
    }
      

    public static void saveDeadlinesWorkflows(String fileName, List<String[]> rows) {
    try (PrintWriter writer = new PrintWriter(new File(fileName))) {
        writer.println("Workflow,Reliability,Cost"); // header
        for (String[] row : rows) {
            writer.println(String.join(",", row));
        }
        System.out.println("CSV written to: " + fileName);
    } catch (FileNotFoundException e) {
        System.err.println("Error writing CSV: " + e.getMessage());
    }
}



    public static Map<String, double[]> computeOC(List<Job> jobs, List<VMData> vms, Map<String, Job> taskMap, List<String> reverseTopological) {
        Map<String, double[]> OC = new HashMap<>();
       

        for (String jobId : reverseTopological) {
            Job task = taskMap.get(jobId);
            double[] oc = new double[vms.size()];

            for (int i = 0; i < vms.size(); i++) {
                VMData vm = vms.get(i);
                double execTime = task.mi / vm.mips;
                double maxChildOC = 0.0;

                for (String childId : task.children) {
                    double minOC = Double.MAX_VALUE;
                    for (int j = 0; j < vms.size(); j++) {
                        double childOC = OC.getOrDefault(childId, new double[vms.size()])[j];
                        double childTime = taskMap.get(childId).mi / vms.get(j).mips;
                        double commTime = (i == j) ? 0 : 0.1; // fake comm time for now
                        double total = childOC + childTime + commTime;
                        minOC = Math.min(minOC, total);
                    }
                    maxChildOC = Math.max(maxChildOC, minOC);
                }
                oc[i] = execTime + maxChildOC;
            }
            OC.put(jobId, oc);
        }
        return OC;
    }

    public static Map<String, Double> rankByOC(Map<String, double[]> OC, List<VMData> vms) {
        Map<String, Double> rank = new HashMap<>();
        for (Map.Entry<String, double[]> entry : OC.entrySet()) {
            double avg = Arrays.stream(entry.getValue()).average().orElse(0.0);
            rank.put(entry.getKey(), avg);
        }
        return rank;
    }

    public static List<Job> sortByRank(List<Job> jobs, Map<String, Double> rank) {
        return jobs.stream()
                .sorted((a, b) -> Double.compare(rank.get(b.id), rank.get(a.id)))
                .collect(Collectors.toList());
    }

    public static ScheduleResult schedule(List<Job> jobs, List<VMData> vms, Map<String, Job> taskMap, double deadline,
        Map<String, double[]> OC ,
        Map<String, Double> rank ,
        List<Job> rankedJobs, double reqrel ) {
        Map<VMData, Double> vmAvail = new HashMap<>();
        Map<VMData, List<Job>> vmUsage = new HashMap<>();
        for (VMData vm : vms) {
            vmAvail.put(vm, 0.0);
            vmUsage.put(vm, new ArrayList<>());
        }

        

        double maxRank = rankedJobs.stream().mapToDouble(r -> rank.get(r.id)).max().orElse(1.0);
        for (Job job : rankedJobs) {
            job.subDeadline = (rank.get(job.id) / maxRank) * deadline;
        }

        List<Job> scheduled = new ArrayList<>();

      for (Job job : rankedJobs) {
    VMData bestVM = null;
    double bestEST = 0.0;
    double bestEFT = Double.MAX_VALUE;
    double bestOEFT = Double.MAX_VALUE;

    for (VMData vm : vms) {
        double est = vmAvail.get(vm);
        double runtime = job.mi / vm.mips;
        double eft = est + runtime;
        double oefT = eft + OC.get(job.id)[vms.indexOf(vm)];

        if (oefT <= job.subDeadline && oefT < bestOEFT) {
            bestVM = vm;
            bestEST = est;
            bestEFT = eft;
            bestOEFT = oefT;
        }
    }

    if (bestVM == null) {
        System.err.println("WARNING: Task " + job.id + " could not be scheduled under deadline.");
        continue;
    }

    // Compute initial reliability of bestVM assignment
    double runtime = job.mi / bestVM.mips;
    double taskRel = getReliability(runtime, LAMBDA);

    double combinedFailureProb = 1.0 - taskRel;
    List<VMData> replicaVMs = new ArrayList<>();

    for (VMData replica : vms) {
        if (replica == bestVM) continue;

        double repRuntime = job.mi / replica.mips;
        double repEst = vmAvail.get(replica);
        double repEFT = repEst + repRuntime;

        // Don't schedule replica beyond deadline
        if (repEFT > job.subDeadline) continue;

        double repRel = getReliability(repRuntime, LAMBDA);
        combinedFailureProb *= (1.0 - repRel);
        replicaVMs.add(replica);

        // Stop if required reliability achieved
        if (1.0 - combinedFailureProb >= reqrel) break;
    }

    double finalRel = 1.0 - combinedFailureProb;
    if (finalRel < reqrel) {
        System.out.printf("WARNING: Task %s reliability is %.6f < %.2f even with replication%n", job.id, finalRel, REQUIRED_RELIABILITY);
    }

    // --- Commit main job to bestVM ---
    job.assignedVM = bestVM;
    job.startTime = bestEST;
    job.endTime = bestEFT;
    vmAvail.put(bestVM, bestEFT);
    vmUsage.get(bestVM).add(job);
    scheduled.add(job);

    // --- Book replicas (optional, track separately if needed) ---
    for (VMData replica : replicaVMs) {
        double repRuntime = job.mi / replica.mips;
        double repEst = vmAvail.get(replica);
        double repEFT = repEst + repRuntime;

        vmAvail.put(replica, repEFT);
        vmUsage.get(replica).add(job); // mark as replica assignment
        // You can also maintain a Map<JobId, List<ReplicaVM>> if needed
    }
}


        double totalCost = 0.0;
        for (Map.Entry<VMData, List<Job>> entry : vmUsage.entrySet()) {
            List<Job> tasks = entry.getValue();
            if (tasks.isEmpty()) continue;
            double start = tasks.stream().mapToDouble(j -> j.startTime).min().orElse(0);
            double end = tasks.stream().mapToDouble(j -> j.endTime).max().orElse(0);
            double usage = end - start;
            double perSecond = entry.getKey().cost / 3600.0;
            totalCost += usage * perSecond;
        }

        return new ScheduleResult(scheduled, totalCost);
    }

    // Utility to get reverse topological order
    public static List<String> getReverseTopological(List<Job> jobs, Map<String, Job> map) {
        List<String> sorted = new ArrayList<>();
        Set<String> visited = new HashSet<>();
        for (Job job : jobs) {
            dfs(job.id, visited, sorted, map);
        }
        Collections.reverse(sorted);
        return sorted;
    }

    private static void dfs(String id, Set<String> visited, List<String> result, Map<String, Job> map) {
        if (visited.contains(id)) return;
        visited.add(id);
        for (String child : map.get(id).children) {
            dfs(child, visited, result, map);
        }
        result.add(id);
    }

    public static double getReliability(double runtime, double lambda) {
        return Math.exp(-lambda * runtime);
    }

}