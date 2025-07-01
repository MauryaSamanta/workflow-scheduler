import java.io.*;
import java.util.*;
import java.nio.file.*;
import javax.xml.parsers.*;
import org.w3c.dom.*;

public class ICPCP_algo {

    static class Job {
        String id;
        List<String> parents = new ArrayList<>();
        List<String> children = new ArrayList<>();
        double mi;
        double startTime, endTime;
        double subDeadline;
        double slack;
        VMData assignedVM;

        Job(String id, double mi) {
            this.id = id;
            this.mi = mi;
        }
    }

    public static void main(String[] args) throws Exception {
        String[] workflows = {"Epigenomics_46.xml", "Inspiral_30.xml", "CyberShake_30.xml"};
        String vmCsv = "cleaned_vm_data.csv";
        List<VMData> vmList = VMData.parseCSV(vmCsv);

        FileWriter csvWriter = new FileWriter("cost_vs_deadline_ICPCP.csv");
        csvWriter.write("workflow,deadline_factor,total_cost\n");

        for (String workflow : workflows) {
            Map<String, Job> taskMap = parseWorkflow(workflow);
            List<String> topoSorted = topologicalSort(taskMap);
            Map<String, Double> upwardRanks = calculateUpwardRanks(topoSorted, taskMap);

            double entryRank = upwardRanks.values().stream().max(Double::compare).orElse(0.0);
            double avgMIPS = vmList.stream().mapToDouble(vm -> vm.mips).average().orElse(1000.0);

            List<Job> upwardSortedJobs = sortUpwardRanks(topoSorted, upwardRanks, taskMap);

            List<VMData> fastVMs = new ArrayList<>(vmList);
            fastVMs.sort((a, b) -> Double.compare(b.mips, a.mips));
            fastVMs = fastVMs.subList(0, fastVMs.size() / 2);

            List<VMData> cheapVMs = new ArrayList<>(vmList);
            cheapVMs.sort(Comparator.comparingDouble(vm -> vm.costperMIPS));
            cheapVMs = cheapVMs.subList(0, cheapVMs.size() / 2);

            for (double df : new double[]{1.0, 1.1,1,2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 2.0}) {
                double userDeadline = df * entryRank;
                for (Job job : taskMap.values()) {
                    double rank = upwardRanks.get(job.id);
                    job.subDeadline = userDeadline - (entryRank - rank);
                    job.slack = job.subDeadline - (job.mi / avgMIPS);
                    job.startTime = 0;
                    job.endTime = 0;
                    job.assignedVM = null;
                }

                double cost = scheduledJobsICPCP(
                    upwardSortedJobs, taskMap, upwardRanks,
                    fastVMs, cheapVMs, userDeadline * 0.1);

                csvWriter.write(workflow.replace(".xml","") + "," + df + "," + cost + "\n");
            }
        }

        csvWriter.close();
    }

    public static Map<String, Job> parseWorkflow(String xmlFile) throws Exception {
        Map<String, Job> taskMap = new HashMap<>();
        Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(new File(xmlFile));
        NodeList jobs = doc.getElementsByTagName("job");
        for (int i = 0; i < jobs.getLength(); i++) {
            Element jobElem = (Element) jobs.item(i);
            String id = jobElem.getAttribute("id");
            double mi = Double.parseDouble(jobElem.getAttribute("runtime")) * 1000; // assuming seconds
            taskMap.put(id, new Job(id, mi));
        }
        NodeList uses = doc.getElementsByTagName("child");
        for (int i = 0; i < uses.getLength(); i++) {
            Element child = (Element) uses.item(i);
            String childId = child.getAttribute("ref");
            NodeList parents = child.getElementsByTagName("parent");
            for (int j = 0; j < parents.getLength(); j++) {
                String parentId = ((Element) parents.item(j)).getAttribute("ref");
                taskMap.get(childId).parents.add(parentId);
                taskMap.get(parentId).children.add(childId);
            }
        }
        return taskMap;
    }

    public static List<String> topologicalSort(Map<String, Job> taskMap) {
        Map<String, Integer> inDegree = new HashMap<>();
        for (String id : taskMap.keySet()) inDegree.put(id, 0);
        for (Job job : taskMap.values()) for (String child : job.children) inDegree.put(child, inDegree.get(child) + 1);
        Queue<String> queue = new LinkedList<>();
        for (String id : inDegree.keySet()) if (inDegree.get(id) == 0) queue.add(id);
        List<String> result = new ArrayList<>();
        while (!queue.isEmpty()) {
            String id = queue.poll();
            result.add(id);
            for (String child : taskMap.get(id).children) {
                inDegree.put(child, inDegree.get(child) - 1);
                if (inDegree.get(child) == 0) queue.add(child);
            }
        }
        return result;
    }

    public static Map<String, Double> calculateUpwardRanks(List<String> sorted, Map<String, Job> map) {
        Map<String, Double> ranks = new HashMap<>();
        for (int i = sorted.size() - 1; i >= 0; i--) {
            String id = sorted.get(i);
            double max = 0;
            for (String child : map.get(id).children) max = Math.max(max, ranks.getOrDefault(child, 0.0));
            ranks.put(id, map.get(id).mi + max);
        }
        return ranks;
    }

    public static List<Job> sortUpwardRanks(List<String> ids, Map<String, Double> ranks, Map<String, Job> map) {
        ids.sort((a, b) -> Double.compare(ranks.get(b), ranks.get(a)));
        List<Job> sorted = new ArrayList<>();
        for (String id : ids) sorted.add(map.get(id));
        return sorted;
    }

    public static double scheduledJobsICPCP(
        List<Job> sorted,
        Map<String, Job> taskMap,
        Map<String, Double> ranks,
        List<VMData> fastVMs,
        List<VMData> cheapVMs,
        double slackThreshold) {

        double cost = 0.0;
        int billingUnit = 3600;
        Map<VMData, List<Job>> vmTasks = new HashMap<>();
        Map<VMData, Double> vmAvailability = new HashMap<>();
        for (VMData vm : fastVMs) vmTasks.put(vm, new ArrayList<>());
        for (VMData vm : cheapVMs) vmTasks.putIfAbsent(vm, new ArrayList<>());

        for (Job job : sorted) {
            List<VMData> pool = (job.slack > slackThreshold) ? cheapVMs : fastVMs;
            VMData best = null;
            double bestEST = 0, bestEFT = Double.MAX_VALUE;
            for (VMData vm : pool) {
                double est = vmAvailability.getOrDefault(vm, 0.0);
                for (String parent : job.parents) est = Math.max(est, taskMap.get(parent).endTime);
                double runtime = job.mi / vm.mips;
                double eft = est + runtime;
                if (eft <= job.subDeadline && eft < bestEFT) {
                    best = vm;
                    bestEST = est;
                    bestEFT = eft;
                }
            }
            if (best != null) {
                job.assignedVM = best;
                job.startTime = bestEST;
                job.endTime = bestEFT;
                vmAvailability.put(best, bestEFT);
                vmTasks.get(best).add(job);
            }
        }

        for (Map.Entry<VMData, List<Job>> entry : vmTasks.entrySet()) {
            List<Job> jobs = entry.getValue();
            if (jobs.isEmpty()) continue;
            jobs.sort(Comparator.comparingDouble(j -> j.startTime));
            double start = jobs.get(0).startTime, end = jobs.get(jobs.size() - 1).endTime;
            double billed = Math.ceil((end - start) / billingUnit) * billingUnit;
            cost += (billed / 3600.0) * entry.getKey().cost;
        }

        return cost;
    }
}
