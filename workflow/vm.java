import java.util.*;

public class VM {
    public String id;
    public double availableAt;
    

    public VM(String id) {
        this.id = id;
        this.availableAt = 0.0;
        
    }

    public VM(String id, double mips) {
        this.id = id;
        this.availableAt = 0.0;
        
    }

    

    // ✅ Static method to generate VMs
    public static List<VM> getDefaultVMs(int count) {
        List<VM> vmList = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            vmList.add(new VM("VM" + i));
        }
        return vmList;
    }
}
