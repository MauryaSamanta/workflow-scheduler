import java.io.*;
import java.util.*;
import java.nio.file.*;

public class VMData {
    public String id;
    public double mips;
    public double cost;
    public double costperMIPS;
    public VMData(String id, double mips, double cost, double costperMIPS) {
        this.id = id;
        this.mips = mips;
        this.cost = cost;
        this.costperMIPS=costperMIPS;
    }

    public static List<VMData> parseCSV(String filePath) {
        List<VMData> vmList = new ArrayList<>();
        try (BufferedReader br = Files.newBufferedReader(Paths.get(filePath))) {
            String line;
            boolean firstLine = true;

            while ((line = br.readLine()) != null) {
                if (firstLine) {
                    firstLine = false;
                    continue; // Skip header
                }

                String[] cols = line.split(",", -1); // -1 handles empty values

                // Ensure enough columns exist
                if (cols.length < 6) continue;

                String apiName = cols[1].trim(); // "API Name"
                
                String vcpuStr = cols[3].trim(); // "vCPUs"
                String costStr = cols[6].trim(); // "On Demand"

                if (apiName.isEmpty() || vcpuStr.isEmpty() || costStr.isEmpty()) continue;

                try {
                    int vcpus = Integer.parseInt(vcpuStr);
                    double cost = Double.parseDouble(costStr);
                    double ghz = 2.5; // You can adjust per instance family
                    double mips = vcpus * ghz * 1000;
                    //System.out.println(mips);
                    double costpermips=cost/mips;
                    vmList.add(new VMData(apiName, mips, cost,costpermips));
                } catch (NumberFormatException e) {
                    // Skip lines with invalid numbers
                    System.out.println(e);
                    continue;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return vmList;
    }

    @Override
    public String toString() {
        return "VMData{id='" + id + "', mips=" + mips + ", cost=" + cost + "}";
    }
}






