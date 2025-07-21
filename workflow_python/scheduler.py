class Scheduler:
    def __init__(self):
        pass

    def schedule_jobs(self, topo_sorted_jobs, job_map, vms_pool, file_size_map, file_to_producer_map):
        vm_availability = {vm['Name']: 0.0 for vm in vms_pool}  # track when each VM becomes free
        vm_job_times = {vm['Name']: [] for vm in vms_pool}  # to track all jobs on each VM
        schedule = []
        wf_finish=0.0
        # topo_sorted_jobs.reverse()

        for job_id in topo_sorted_jobs:
            # print(job_id)
            job = job_map[job_id]
            best_vm = None
            best_cost = float('inf')
            best_est = 0.0
            best_eft = 0.0

            for vm in vms_pool:
                vm_name = vm['Name']
                mips = vm['mips']
                cost_per_sec = vm['cost'] / 3600.0
                vm_ready = vm_availability[vm_name]
                vm_band = vm['Network Performance'] * 125.0

                data_ready = 0.0
                for parent_id in job['parents']:
                    parent = job_map[parent_id]
                    comm_time = 0.0

                    parent_output = {f['file'] for f in parent['output_files']}
                    job_input = {f['file'] for f in job['input_files']}
                    shared = parent_output & job_input
                    total_size = sum(file_size_map.get(f, 0.0) for f in shared)

                    if parent.get('vm_name') != vm_name:
                        comm_time = total_size / vm_band if vm_band else 0.0

                    data_ready = max(data_ready, parent['end_time'] + comm_time)

                est = max(vm_ready, data_ready)
                runtime = job['mi'] / mips
                eft = est + runtime
                # print(job['id'], est,eft, job['sub_deadline'])
                if eft <= job['sub_deadline']:
                    total_cost = cost_per_sec * (eft - est)

                    if total_cost < best_cost:
                        best_cost = total_cost
                        best_vm = vm_name
                        best_est = est
                        best_eft = eft
                        wf_finish=best_eft
                    # print(job['id'], best_cost, best_vm, best_est, best_eft)
            if best_vm is not None:
                job['vm_name'] = best_vm
                job['start_time'] = best_est
                job['end_time'] = best_eft
                vm_availability[best_vm] = best_eft
                vm_job_times[best_vm].append((best_est, best_eft))
                schedule.append(job)
            else:
                print(f"⚠️ WARNING: Job {job_id} could not be scheduled within sub-deadline.")
                break

        # ✅ Final cost calculation per VM
        total_cost = 0.0
        for vm in vms_pool:
            vm_name = vm['Name']
            jobs_on_vm = vm_job_times[vm_name]
            if jobs_on_vm:
                vm_start = min(start for start, end in jobs_on_vm)
                vm_end = max(end for start, end in jobs_on_vm)
                usage_seconds = vm_end - vm_start
                cost_per_sec = vm['cost'] / 3600.0
                vm_cost = usage_seconds * cost_per_sec
                total_cost += vm_cost

        return total_cost, schedule,wf_finish

