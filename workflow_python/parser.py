import xml.etree.ElementTree as ET

class WorkflowParser:

    def __init__(self):
        pass

    def parsefromXml(self, filePath: str, median_mips):
        jobs = []
        file_to_producer_map = {}
        file_size_map = {}
        try:
            tree = ET.parse(filePath)
            root = tree.getroot()

            # Extract the namespace dynamically
            ns = {'ns': root.tag.split('}')[0].strip('{')}  # e.g., {'ns': 'http://pegasus.isi.edu/schema/DAX'}

            for job_element in root.findall('ns:job', ns):
                job_details = {}

                job_details['id'] = job_element.get('id')
                job_details['runtime'] = float(job_element.get('runtime'))
                job_details['name'] = job_element.get('name')
                job_details['namespace'] = job_element.get('namespace')
                job_details['version'] = job_element.get('version')
                job_details['mi']=job_details['runtime']*median_mips
                input_files = []
                output_files = []
                for uses_element in job_element.findall('ns:uses', ns):
                    file_info = {
                        'file': uses_element.get('file'),
                        'link': uses_element.get('link'),
                        'type': uses_element.get('type'),
                        'size': int(uses_element.get('size')) if uses_element.get('size') else None
                    }
                    if uses_element.get('link') == 'input':
                        input_files.append(file_info)
                        
                        # print(job_element.get('id'),file_info)
                        file_size_map[uses_element.get('file')] = int(uses_element.get('size'))
                    elif uses_element.get('link') == 'output':
                        output_files.append(file_info)
                        file_to_producer_map[uses_element.get('file')] = job_details['id']
                        file_size_map[uses_element.get('file')] = int(uses_element.get('size'))

                job_details['input_files'] = input_files
                # print(job_element.get('id'),input_files)
                job_details['output_files'] = output_files

                # Parents
                parent_refs = []
                for child in root.findall('ns:child', ns):
                    if child.get('ref') == job_details['id']:
                        for parent in child.findall('ns:parent', ns):
                            parent_refs.append(parent.get('ref'))
                job_details['parents'] = parent_refs

                # Children
                child_refs = []
                for child in root.findall('ns:child', ns):
                    for parent in child.findall('ns:parent', ns):
                        if parent.get('ref') == job_details['id']:
                            child_refs.append(child.get('ref'))
                job_details['children'] = child_refs

                jobs.append(job_details)

        except ET.ParseError as e:
            print(f"Error parsing XML: {e}")

        return jobs, file_to_producer_map, file_size_map
