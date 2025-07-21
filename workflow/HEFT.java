import java.io.*;
import java.nio.file.*;
import java.util.*;


public class HEFT {
     static class VMData {
        public String id;
        public double mips;
        public double cost;
        public double costPerMIPS;
        public double networkPerformance;

        public VMData(String id, double mips, double cost, double costPerMIPS, double networkPerformance) {
            this.id = id;
            this.mips = mips;
            this.cost = cost;
            this.costPerMIPS = costPerMIPS;
            this.networkPerformance = networkPerformance;
        }

        public static List<VMData> parseCSV(String filePath) {
            List<VMData> vmList = new ArrayList<>();
            try (BufferedReader br = Files.newBufferedReader(Paths.get(filePath))) {
                String line;
                boolean firstLine = true;
                while ((line = br.readLine()) != null) {
                    if (firstLine) {
                        firstLine = false;
                        continue;
                    }
                    String[] cols = line.split(",", -1);
                    if (cols.length < 7) continue;
                    try {
                        String apiName = cols[1].trim();
                        int vcpus = Integer.parseInt(cols[3].trim());
                        double cost = Double.parseDouble(cols[6].trim());
                        double bandwidth = Double.parseDouble(cols[5].trim());
                        double mips = vcpus * 2.5 * 1000;
                        double costPerMIPS = cost / mips;
                        vmList.add(new VMData(apiName, mips, cost, costPerMIPS, bandwidth));
                    } catch (Exception ignored) {}
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return vmList;
        }
    }

    static class Job {
        public String id;
        public double mi;
        public List<String> parents = new ArrayList<>();
        public double deadline;

        public Job(String id, double mi, double deadline) {
            this.id = id;
            this.mi = mi;
            this.deadline = deadline;
        }
    }

    public static void main(String[] args) throws Exception {
        String[] workflows = {"Epigenomics_46.xml", "Inspiral_30.xml", "CyberShake_30.xml"};
        String vmCsv = "cleaned_vm_data.csv";
        List<VMData> vmList = VMData.parseCSV(vmCsv);

        FileWriter csvWriter = new FileWriter("workflow-wise-costs-HEFT-V1.csv");
        csvWriter.write("workflow,deadline_factor,total_cost\n");

        for (String workflow : workflows) {
            Map<String, Job> taskMap = parseWorkflow(workflow);
            List<String> sortedTasks = topologicalSort(taskMap);
            List<Job> taskList = new ArrayList<>();
            for (String id : sortedTasks) taskList.add(taskMap.get(id));

            double maxMI = taskList.stream().mapToDouble(j -> j.mi).sum();
            double avgMIPS = vmList.stream().mapToDouble(vm -> vm.mips).average().orElse(1000);

            for (double df : new double[]{1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 2.0}) {
                double deadline = (maxMI / avgMIPS) * df;
                double totalCost = runCostAwareHEFT(taskList, vmList, deadline);
                csvWriter.write(workflow + "," + df + "," + totalCost + "\n");
            }
        }
        csvWriter.close();
    }

    static double runCostAwareHEFT(List<Job> tasks, List<VMData> vms, double deadline) {
        Map<Job, Double> rank = new HashMap<>();
        for (Job task : tasks) {
            double avgCost = vms.stream().mapToDouble(vm -> (task.mi / vm.mips) * (vm.cost / 3600)).average().orElse(1);
            rank.put(task, avgCost);
        }

        tasks.sort((a, b) -> Double.compare(rank.get(b), rank.get(a)));
        Map<VMData, Double> vmTime = new HashMap<>();
        for (VMData vm : vms) vmTime.put(vm, 0.0);

        double totalCost = 0.0;
        for (Job task : tasks) {
            double minCost = Double.MAX_VALUE;
            VMData bestVM = null;
            double bestStart = 0.0;
            double bestEnd = 0.0;

            for (VMData vm : vms) {
                double start = vmTime.get(vm);
                double runtime = task.mi / vm.mips;
                double end = start + runtime;
                double cost = (runtime) * (vm.cost / 3600);
                if (end <= task.deadline && cost < minCost) {
                    minCost = cost;
                    bestVM = vm;
                    bestStart = start;
                    bestEnd = end;
                }
            }

            if (bestVM != null) {
                vmTime.put(bestVM, bestEnd);
                totalCost += minCost;
            }
        }

        return totalCost;
    }

    // Dummy placeholder to parse workflows
    static Map<String, Job> parseWorkflow(String filename) {
        Map<String, Job> map = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            Job job = new Job("T" + i, 1000 + i * 100, Double.MAX_VALUE);
            if (i > 0) job.parents.add("T" + (i - 1));
            map.put(job.id, job);
        }
        return map;
    }

    static List<String> topologicalSort(Map<String, Job> jobMap) {
        List<String> result = new ArrayList<>();
        Set<String> visited = new HashSet<>();

        for (String id : jobMap.keySet()) dfs(id, visited, result, jobMap);
        Collections.reverse(result);
        return result;
    }

    static void dfs(String id, Set<String> visited, List<String> result, Map<String, Job> map) {
        if (visited.contains(id)) return;
        visited.add(id);
        for (String parent : map.get(id).parents) dfs(parent, visited, result, map);
        result.add(id);
    }
}
