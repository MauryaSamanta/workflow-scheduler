import javax.xml.parsers.*;
import org.w3c.dom.*;
import java.io.File;
import java.util.*;

public class DAG {

    public static class Job {
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
        double upwardRank;

        public Job(String id, double runtime, double mi, List<String> inputFiles, List<String> outputFiles) {
            this.id = id;
            this.runtime = runtime;
            this.mi = mi;
            this.subDeadline = 0.0;
            this.slack = 0.0;
            this.startTime = 0.0;
            this.endTime = 0.0;
            this.assignedVM = null;
            this.inputFiles = inputFiles;
            this.outputFiles = outputFiles;
            this.upwardRank = -1;
        }
    }

    public static List<Job> parseDAGFromXML(String filePath, List<VMData> vmList,
                                            Map<String, String> fileToProducerTaskMap,
                                            Map<String, Double> fileSizeMap,
                                            Map<String, Job> taskMapOut) {
        List<Job> jobList = new ArrayList<>();
        try {
            File xmlFile = new File(filePath);
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(xmlFile);
            doc.getDocumentElement().normalize();

            double medianMIPS = getMedianMIPS(vmList);

            NodeList jobs = doc.getElementsByTagNameNS("*", "job");
            for (int i = 0; i < jobs.getLength(); i++) {
                Element jobEl = (Element) jobs.item(i);
                String jobId = jobEl.getAttribute("id");
                double runtime = Double.parseDouble(jobEl.getAttribute("runtime"));
                double mi = runtime * medianMIPS;

                List<String> inputFiles = new ArrayList<>();
                List<String> outputFiles = new ArrayList<>();

                NodeList usesList = jobEl.getElementsByTagName("uses");
                for (int j = 0; j < usesList.getLength(); j++) {
                    Element uses = (Element) usesList.item(j);
                    String fileName = uses.getAttribute("file");
                    String linkType = uses.getAttribute("link");
                    String sizeStr = uses.getAttribute("size");
                    double size = sizeStr.isEmpty() ? 0.0 : Double.parseDouble(sizeStr);

                    if (linkType.equalsIgnoreCase("output")) {
                        fileToProducerTaskMap.put(fileName, jobId);
                        fileSizeMap.put(fileName, size);
                        outputFiles.add(fileName);
                    } else if (linkType.equalsIgnoreCase("input")) {
                        inputFiles.add(fileName);
                    }
                }

                Job job = new Job(jobId, runtime, mi, inputFiles, outputFiles);
                taskMapOut.put(jobId, job);
            }

            NodeList childList = doc.getElementsByTagNameNS("*", "child");
            for (int i = 0; i < childList.getLength(); i++) {
                Element child = (Element) childList.item(i);
                String childId = child.getAttribute("ref");

                NodeList parents = child.getElementsByTagNameNS("*", "parent");
                for (int j = 0; j < parents.getLength(); j++) {
                    Element parent = (Element) parents.item(j);
                    String parentId = parent.getAttribute("ref");
                    taskMapOut.get(parentId).children.add(childId);
                    taskMapOut.get(childId).parents.add(parentId);
                }
            }

            jobList = new ArrayList<>(taskMapOut.values());

            computeUpwardRanks(taskMapOut, medianMIPS);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return jobList;
    }

    private static double getMedianMIPS(List<VMData> vmList) {
        List<Double> mipsList = new ArrayList<>();
        for (VMData vm : vmList) {
            mipsList.add(vm.mips);
        }
        Collections.sort(mipsList);
        int n = mipsList.size();
        return (n % 2 == 0) ? (mipsList.get(n / 2 - 1) + mipsList.get(n / 2)) / 2.0 : mipsList.get(n / 2);
    }

    private static void computeUpwardRanks(Map<String, Job> taskMap, double avgMIPS) {
        for (Job job : taskMap.values()) {
            job.upwardRank = computeUpwardRank(job, taskMap, avgMIPS);
        }
    }

    private static double computeUpwardRank(Job job, Map<String, Job> taskMap, double avgMIPS) {
        if (job.upwardRank >= 0) return job.upwardRank;
        double execTime = job.mi / avgMIPS;
        if (job.children.isEmpty()) {
            job.upwardRank = execTime;
        } else {
            double maxChildRank = 0;
            for (String childId : job.children) {
                Job child = taskMap.get(childId);
                maxChildRank = Math.max(maxChildRank, computeUpwardRank(child, taskMap, avgMIPS));
            }
            job.upwardRank = execTime + maxChildRank;
        }
        return job.upwardRank;
    }
}
