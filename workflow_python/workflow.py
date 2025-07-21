from parser import WorkflowParser
from upward_rank import UpwardRankCalculator,find_critical_path,compute_est_eft,assign_subdeadlines,topological_sort,topological_sort_total
from scheduler import Scheduler
import pandas as pd
class Workflow:

    def __init__(self):
        pass

    def main():
        vms = pd.read_csv("cleaned_vm_data.csv")
        vms["mips"] = vms["vCPUs"] * 2.5 * 1000
        vms['cost'] = vms['On Demand']
        median_mips = vms["mips"].mean()

        wfp = WorkflowParser()
        workflow_name = "CyberShake_30.xml"
        jobs, file_to_producer_map, file_size_map = wfp.parsefromXml(workflow_name, median_mips)
        bandwidth = vms["Network Performance"].mean()
        vms_pool = vms.to_dict(orient='records') 

        compute_est_eft(jobs, {j['id']: j for j in jobs}, bandwidth, file_size_map, file_to_producer_map)
        topo_sorted_jobs_all = topological_sort_total(jobs)
        urc = UpwardRankCalculator(topo_sorted_jobs_all)
        jobs_with_ranks = urc.compute_all_upward_ranks()

        job_map = {job['id']: job for job in jobs}
        critical_path = find_critical_path(jobs, job_map, urc.rank_cache)
        cp_length = max(urc.rank_cache.values())

        base_deadline = 1000
        results = []

        for multiplier in [round(1.0 + 0.1 * i, 1) for i in range(1, 11)]:
            deadline = base_deadline * multiplier

            # Deep copy the jobs to avoid state leakage
            import copy
            jobs_copy = copy.deepcopy(jobs)
            job_map_copy = {j['id']: j for j in jobs_copy}

            compute_est_eft(jobs_copy, job_map_copy, bandwidth, file_size_map, file_to_producer_map)
            assign_subdeadlines(jobs_copy, job_map_copy, critical_path, deadline, cp_length, file_size_map, bandwidth)
            topo_sorted_jobs = topological_sort(jobs_copy)

            scheduler = Scheduler()
            try:
                total_cost, schedule, wf_finish = scheduler.schedule_jobs(topo_sorted_jobs, job_map_copy, vms_pool, file_size_map, file_to_producer_map)
                results.append({
                    "workflow": workflow_name,
                    "deadline_multiplier": multiplier,
                    "absolute_deadline": deadline,
                    "total_cost": total_cost,
                    "makespan": wf_finish
                })
            except Exception as e:
                print(f"Scheduling failed for multiplier {multiplier}: {e}")
                results.append({
                    "workflow": workflow_name,
                    "deadline_multiplier": multiplier,
                    "absolute_deadline": deadline,
                    "total_cost": "FAILED",
                    "makespan": "FAILED"
                })
        print(results)
        # Save to CSV
        df = pd.DataFrame(results)
        df.to_csv("deadline_vs_cost.csv", index=False)
        print("Saved deadline_vs_cost.csv")
    if __name__ == "__main__":
        main()

        
    