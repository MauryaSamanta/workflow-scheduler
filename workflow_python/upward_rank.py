class UpwardRankCalculator:
    def __init__(self, jobs):
        self.jobs = jobs
        self.job_map =  {job['id']: job for job in jobs}
        self.rank_cache = {}

    def compute_all_upward_ranks(self):
        for job in self.jobs:
            
            job_id = job['id']
            print(job_id)
            rank = self._compute_upward_rank(job_id)
            job['upward_rank'] = rank
        return self.jobs  # Updated list with 'upward_rank'

    def _compute_upward_rank(self, job_id):
        if job_id in self.rank_cache:
            return self.rank_cache[job_id]

        job = self.job_map[job_id]
        runtime = job['runtime']

        if not job.get('children'):  # Leaf node
            self.rank_cache[job_id] = runtime
        else:
            max_child_rank = max(self._compute_upward_rank(child_id) for child_id in job['children'])
            self.rank_cache[job_id] = runtime + max_child_rank

        return self.rank_cache[job_id]
    
def find_critical_path(jobs, job_map, upward_ranks):
    max_entry = max(upward_ranks.items(), key=lambda x: x[1])[0]
    path = []

    def dfs(job_id):
        path.append(job_id)
        children = job_map[job_id]['children']
        if not children:
            return
        next_child = max(children, key=lambda c: upward_ranks[c])
        dfs(next_child)

    dfs(max_entry)
    return path
    
def compute_est_eft(jobs, job_map, bandwidth, file_size_map, file_to_producer_map):
    sorted_ids = topological_sort(jobs)
    sorted_ids.reverse()
    for job_id in sorted_ids:
        job=job_map[job_id]
        # print(job)
        est = 0.0
        for parent_id in job['parents']:
            parent = job_map[parent_id]
            comm_time = 0.0

            # ✅ Extract file names from dicts
            parent_output_files = {f['file'] for f in parent['output_files']}
            job_input_files = {f['file'] for f in job['input_files']}

            shared_files = parent_output_files & job_input_files
            total_size = sum(file_size_map.get(f, 0.0) for f in shared_files)

            comm_time = total_size / bandwidth if bandwidth else 0.0
            
            est = max(est, parent.get('EFT', 0.0) + comm_time)
        # print(est)
        job['EST'] = est
        job['EFT'] = est + job['runtime']

def assign_subdeadlines(
    jobs, job_map, critical_path, deadline, cp_length, file_size_map, bandwidth
):
    """
    Assigns sub-deadlines to each job in the workflow.
    - Critical path jobs get proportional slack.
    - Non-critical jobs get sub-deadlines based on children and communication times.
    Ensures that no sub-deadline exceeds the overall deadline.
    """

    slack = deadline - cp_length

    # Step 1: Assign sub-deadlines for critical path jobs
    for job_id in critical_path:
        job = job_map[job_id]
        wi = job['runtime']
        if cp_length > 0:
            proportional_sd = job['EST'] + (wi / cp_length) * slack
        else:
            proportional_sd = job['EST']
        job['sub_deadline'] = min(deadline, max(job['EST'] + job['runtime'], proportional_sd))

    # Step 2: Assign sub-deadlines for non-critical path jobs
    sorted_ids = topological_sort(jobs)
    sorted_ids.reverse()  # Bottom-up

    for job_id in sorted_ids:
        if job_id in critical_path:
            continue

        job = job_map[job_id]
        min_child_sd = float('inf')
        has_child_sd = False

        for child_id in job['children']:
            child = job_map[child_id]

            parent_output_files = {f['file'] for f in job.get('output_files', [])}
            child_input_files = {f['file'] for f in child.get('input_files', [])}
            shared_files = parent_output_files & child_input_files

            total_size = sum(file_size_map.get(f, 0.0) for f in shared_files)
            comm_time = total_size / bandwidth if bandwidth else 0.0

            child_sd = child.get('sub_deadline')
            if child_sd is not None:
                min_child_sd = min(min_child_sd, child_sd - comm_time)
                has_child_sd = True

        if has_child_sd and min_child_sd != float('inf'):
            raw_sd = max(job['EST'] + job['runtime'], min_child_sd)
        else:
            # Fallback: upward-rank-based proportional allocation
            rank = job.get('upward_rank', 0.0)
            if not job['children']:  # It's a leaf job
                raw_sd = deadline
            elif cp_length > 0:
                raw_sd = job['EST'] + (rank / cp_length) * slack
            else:
                raw_sd = job['EST'] + job['runtime']

        # Final constraints
        job['sub_deadline'] = min(deadline, max(job['EST'] + job['runtime'], raw_sd))


def topological_sort(jobs):
    from collections import defaultdict, deque
    # print(jobs)
    
    in_degree = defaultdict(int)
    graph = defaultdict(list)
    job_ids = {job['id'] for job in jobs}

    for job in jobs:
        for child in job['children']:
            graph[job['id']].append(child)
            in_degree[child] += 1

    queue = deque([job_id for job_id in job_ids if in_degree[job_id] == 0])
    sorted_order = []

    while queue:
        curr = queue.popleft()
        sorted_order.append(curr)
        for neighbor in graph[curr]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    return sorted_order  # This is topological order

def topological_sort_total(jobs):
    from collections import defaultdict, deque

    in_degree = defaultdict(int)
    graph = defaultdict(list)
    job_map = {job['id']: job for job in jobs}  # Map job ID to full job dict

    for job in jobs:
        for child in job['children']:
            graph[job['id']].append(child)
            in_degree[child] += 1

    queue = deque([job['id'] for job in jobs if in_degree[job['id']] == 0])
    sorted_jobs = []

    while queue:
        curr_id = queue.popleft()
        sorted_jobs.append(job_map[curr_id])
        for neighbor in graph[curr_id]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    return sorted_jobs  # Return full job dicts instead of just IDs


            
