import java.util.*;

public class My_algo {

    // ----- Job Class -----
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

    // ----- VM Type Class -----
    static class VMType {
        String name;
        double mips;
        double costPerHour;
        double bootTime; // seconds
        int quantity;

        public VMType(String name, double mips, double costPerHour, double bootTime, int quantity) {
            this.name = name;
            this.mips = mips;
            this.costPerHour = costPerHour;
            this.bootTime = bootTime;
            this.quantity = quantity;
        }
    }

    // ----- VM Instance Class -----
    static class VMInstance {
        String id;
        VMType type;
        double availableAt;
        double bootedAt;

        public VMInstance(String id, VMType type, double availableAt, double bootedAt) {
            this.id = id;
            this.type = type;
            this.availableAt = availableAt;
            this.bootedAt = bootedAt;
        }
    }

    // ----- Upward Rank Calculation -----
    public static void computeUpwardRanks(Map<String, Job> taskMap, List<VMType> vmTypes) {
        double avgMIPS = vmTypes.stream().mapToDouble(vm -> vm.mips).average().orElse(1000);
        for (Job job : taskMap.values()) {
            job.upwardRank = computeRankRecursively(job, taskMap, avgMIPS);
        }
    }

    private static double computeRankRecursively(Job job, Map<String, Job> taskMap, double avgMIPS) {
        if (job.upwardRank >= 0) return job.upwardRank;
        double execTime = job.mi / avgMIPS;
        if (job.children.isEmpty()) {
            job.upwardRank = execTime;
        } else {
            double maxChild = 0;
            for (String childId : job.children) {
                Job child = taskMap.get(childId);
                maxChild = Math.max(maxChild, computeRankRecursively(child, taskMap, avgMIPS));
            }
            job.upwardRank = execTime + maxChild;
        }
        return job.upwardRank;
    }

    // ----- Main Scheduler -----
    public static List<Job> scheduleCEDA(List<Job> allJobs, List<VMType> vmTypes, Map<String, Job> taskMap) {
        computeUpwardRanks(taskMap, vmTypes);
        List<Job> sortedJobs = new ArrayList<>(allJobs);
        sortedJobs.sort((j1, j2) -> Double.compare(j2.upwardRank, j1.upwardRank));

        Map<VMType, List<VMInstance>> vmPool = new HashMap<>();
        List<Job> finalSchedule = new ArrayList<>();
        int vmIdCounter = 1;

        for (VMType type : vmTypes) {
            vmPool.put(type, new ArrayList<>());
        }

        for (Job job : sortedJobs) {
            double est = 0.0;
            for (String parentId : job.parents) {
                Job parent = taskMap.get(parentId);
                if (parent != null) {
                    est = Math.max(est, parent.endTime);
                }
            }

            VMInstance bestVM = null;
            double bestCost = Double.MAX_VALUE;
            double bestStart = 0, bestEnd = 0;

            for (VMType type : vmTypes) {
                double runtime = job.mi / type.mips;
                double cost = (Math.ceil(runtime / 3600.0)) * type.costPerHour;

                for (VMInstance vm : vmPool.get(type)) {
                    double start = Math.max(vm.availableAt, est);
                    double end = start + runtime;

                    if (end <= job.subDeadline && cost < bestCost) {
                        bestCost = cost;
                        bestVM = vm;
                        bestStart = start;
                        bestEnd = end;
                    }
                }

                if (bestVM == null && type.quantity > 0) {
                    double start = est + type.bootTime;
                    double end = start + runtime;

                    if (end <= job.subDeadline && cost < bestCost) {
                        String vmId = "vm" + (vmIdCounter++);
                        VMInstance newVM = new VMInstance(vmId, type, end, start - runtime);
                        bestVM = newVM;
                        bestStart = start;
                        bestEnd = end;
                    }
                }
            }

            if (bestVM == null) {
                System.out.println("Job " + job.id + " could not be scheduled within deadline.");
                continue;
            }

            job.assignedVM = bestVM;
            job.startTime = bestStart;
            job.endTime = bestEnd;

            if (!vmPool.get(bestVM.type).contains(bestVM)) {
                vmPool.get(bestVM.type).add(bestVM);
            }

            bestVM.availableAt = bestEnd;
            finalSchedule.add(job);
        }

        return finalSchedule;
    }

    // ----- Main Method -----
    public static void main(String[] args) {
        List<VMType> vmTypes = new ArrayList<>();
        vmTypes.add(new VMType("c5.large", 2000, 0.085, 60, 5));
        vmTypes.add(new VMType("m5.xlarge", 3000, 0.15, 90, 5));

        List<Job> jobs = new ArrayList<>();
        Map<String, Job> taskMap = new HashMap<>();

        String file = "../config/dax/Cyber_shake_30.xml";
File xmlFile = new File(file);
DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
factory.setNamespaceAware(true);
DocumentBuilder builder = factory.newDocumentBuilder();
Document doc = builder.parse(xmlFile);
doc.getDocumentElement().normalize();

Map<String, Job> taskMap = new HashMap<>();
Map<String, String> fileToProducerTaskMap = new HashMap<>();
Map<String, Double> fileSizeMap = new HashMap<>();
List<VMType> vmTypes = VMType.loadFromCSV("cleaned_vm_data.csv");

double median_mips = getMedianMIPS(vmTypes);

NodeList jobList = doc.getElementsByTagNameNS("*", "job");
for (int i = 0; i < jobList.getLength(); i++) {
    Element job = (Element) jobList.item(i);
    String jobId = job.getAttribute("id");
    double runtime = Double.parseDouble(job.getAttribute("runtime"));
    double mi = runtime * median_mips;

    List<String> inputFiles = new ArrayList<>();
    List<String> outputFiles = new ArrayList<>();

    NodeList usesList = job.getElementsByTagName("uses");
    for (int j = 0; j < usesList.getLength(); j++) {
        Element uses = (Element) usesList.item(j);
        String file = uses.getAttribute("file");
        String type = uses.getAttribute("link");
        double size = uses.hasAttribute("size") ? Double.parseDouble(uses.getAttribute("size")) : 0.0;

        if (type.equalsIgnoreCase("input")) inputFiles.add(file);
        if (type.equalsIgnoreCase("output")) {
            outputFiles.add(file);
            fileToProducerTaskMap.put(file, jobId);
            fileSizeMap.put(file, size);
        }
    }

    Job newJob = new Job(jobId, runtime, mi, inputFiles, outputFiles);
    taskMap.put(jobId, newJob);
}

NodeList childList = doc.getElementsByTagNameNS("*", "child");
for (int i = 0; i < childList.getLength(); i++) {
    Element child = (Element) childList.item(i);
    String childId = child.getAttribute("ref");

    NodeList parentList = child.getElementsByTagNameNS("*", "parent");
    for (int j = 0; j < parentList.getLength(); j++) {
        String parentId = ((Element) parentList.item(j)).getAttribute("ref");
        taskMap.get(childId).parents.add(parentId);
        taskMap.get(parentId).children.add(childId);
    }
}

        List<Job> allJobs = new ArrayList<>(taskMap.values());
List<Job> schedule = scheduleCEDA(allJobs, vmTypes, taskMap);


        System.out.println("\nCEDA Scheduling Result:");
        for (Job j : schedule) {
            System.out.println("Task " + j.id +
                " → VM: " + j.assignedVM.id +
                " [" + j.assignedVM.type.name + "]" +
                " | Start: " + j.startTime +
                " | End: " + j.endTime +
                " | Rank: " + j.upwardRank);
        }
    }
    public static double getMedianMIPS(List<VMType> vmTypes) {
    List<Double> mipsValues = vmTypes.stream().map(vm -> vm.mips).sorted().toList();
    int n = mipsValues.size();
    return (n % 2 == 0) ? (mipsValues.get(n / 2 - 1) + mipsValues.get(n / 2)) / 2.0 : mipsValues.get(n / 2);
}

public static double getMedianBW(List<VMType> vmTypes) {
    // You can define networkPerformance field in VMType if needed
    return 125.0; // placeholder for now
}

}
