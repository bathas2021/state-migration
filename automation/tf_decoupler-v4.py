import json
import csv
import os
import argparse
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

class TerraformStateDecoupler:
    def __init__(self, state_file='default.tfstate'):
        self.state_file = state_file
        self.state_data = None
        self.lock = threading.Lock()  # Prevent concurrent writes to the same state file

    def load_and_clean_state(self):
        """Loads the state file, automatically cleaning trailing garbage if necessary."""
        if not os.path.exists(self.state_file):
            print(f"❌ Error: '{self.state_file}' not found.")
            return False

        with open(self.state_file, 'r') as f:
            content = f.read().strip()

        try:
            self.state_data = json.loads(content)
            print(f"✅ Successfully loaded clean JSON from {self.state_file}.")
        except json.JSONDecodeError:
            print("⚠️ Strict JSON parsing failed. Attempting to extract valid JSON...")
            try:
                decoder = json.JSONDecoder()
                self.state_data, index = decoder.raw_decode(content)
                print(f"✅ Successfully recovered JSON.")
            except Exception as e:
                print(f"❌ FATAL ERROR: Could not parse state file. {e}")
                return False
        return True

    def _get_old_address(self, module_prefix, res_type, res_name, instance):
        address_parts = [module_prefix] if module_prefix else []
        base_addr = f"{res_type}.{res_name}"
        if 'index_key' in instance:
            idx = instance['index_key']
            base_addr += f'["{idx}"]' if isinstance(idx, str) else f'[{idx}]'
        address_parts.append(base_addr)
        return ".".join(address_parts)

    def generate_mapping_csv(self, resource_types, output_csv='migration_plan.csv', new_module_prefix='module.core'):
        if not self.state_data: return
        mappings = []
        for resource in self.state_data.get('resources', []):
            res_type = resource.get('type')
            if resource.get('mode') == 'managed' and res_type in resource_types:
                module_prefix = resource.get('module', '')
                res_name = resource.get('name')
                for instance in resource.get('instances', []):
                    old_addr = self._get_old_address(module_prefix, res_type, res_name, instance)
                    index_suffix = ""
                    if 'index_key' in instance:
                        idx = instance['index_key']
                        index_suffix = f'["{idx}"]' if isinstance(idx, str) else f'[{idx}]'
                    new_addr = f"{new_module_prefix}.{res_type}.{res_name}{index_suffix}"
                    mappings.append({"Resource_Type": res_type, "Old_Address": old_addr, "New_Address": new_addr})

        with open(output_csv, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=["Resource_Type", "Old_Address", "New_Address"])
            writer.writeheader()
            writer.writerows(mappings)
        print(f"✅ Generated '{output_csv}' with {len(mappings)} resources.")

    def _move_worker(self, old_addr, new_addr, target_state, current_idx, total):
        """Single worker task for moving state."""
        # The lock ensures only one 'terraform state mv' command runs at a time
        # This is mandatory to prevent state file corruption.
        with self.lock:
            cmd = [
                "terraform", "state", "mv",
                f"-backup={os.devnull}", f"-backup-out={os.devnull}",
                f"-state={self.state_file}", f"-state-out={target_state}",
                old_addr, new_addr
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
        if result.returncode == 0:
            return True, f"[{current_idx}/{total}] ✅ Moved: {old_addr}"
        else:
            return False, f"[{current_idx}/{total}] ❌ Failed: {old_addr} - {result.stderr.strip()}"

    def execute_moves(self, csv_file, target_state, max_workers=5):
        if not os.path.exists(csv_file): return
        with open(csv_file, 'r') as in_f:
            rows = [row for row in csv.DictReader(in_f) if row.get('New_Address', '').strip()]

        total = len(rows)
        print(f"🚀 Starting Multi-threaded Migration ({max_workers} workers)...\n")
        
        success_count = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(self._move_worker, r['Old_Address'], r['New_Address'], target_state, i, total)
                for i, r in enumerate(rows, 1)
            ]
            for future in as_completed(futures):
                success, message = future.result()
                print(message)
                if success: success_count += 1

        print(f"\n🎉 Successfully moved {success_count} of {total} resources.")
        self._scrub_target_state(target_state)

    def _scrub_target_state(self, target_state):
        """Erases dependencies from the new state file."""
        if not os.path.exists(target_state): return
        print(f"\n🧹 Scrubbing dependencies from '{target_state}'...")
        with open(target_state, 'r') as f:
            data = json.load(f)
        
        count = 0
        for res in data.get('resources', []):
            for inst in res.get('instances', []):
                if inst.get('dependencies'):
                    inst['dependencies'] = []
                    count += 1
        
        with open(target_state, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"✅ Scrubbed {count} instances.")

    def execute_rm(self, csv_file, max_workers=5):
        if not os.path.exists(csv_file): return
        with open(csv_file, 'r') as in_f:
            rows = [row for row in csv.DictReader(in_f) if row.get('Old_Address', '').strip()]

        total = len(rows)
        print(f"🗑️ Starting Multi-threaded Removal ({max_workers} workers)...\n")
        
        success_count = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for i, r in enumerate(rows, 1):
                old_addr = r['Old_Address']
                futures.append(executor.submit(self._rm_worker, old_addr, i, total))
            
            for future in as_completed(futures):
                success, message = future.result()
                print(message)
                if success: success_count += 1

        print(f"\n🎉 Successfully removed {success_count} of {total} resources.")

    def _rm_worker(self, addr, current_idx, total):
        with self.lock:
            cmd = ["terraform", "state", "rm", f"-backup={os.devnull}", f"-state={self.state_file}", addr]
            result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            return True, f"[{current_idx}/{total}] ✅ Removed: {addr}"
        return False, f"[{current_idx}/{total}] ❌ Failed: {addr}"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Multi-threaded Terraform Migration Tool")
    parser.add_argument('--state', default='default.tfstate')
    parser.add_argument('--workers', type=int, default=5, help="Number of parallel threads")
    
    subparsers = parser.add_subparsers(dest='command')

    map_p = subparsers.add_parser('map')
    map_p.add_argument('--types', nargs='+', required=True)
    map_p.add_argument('--csv', default='migration_plan.csv')
    map_p.add_argument('--prefix', default='module.core')

    move_p = subparsers.add_parser('move')
    move_p.add_argument('--csv', default='migration_plan.csv')
    move_p.add_argument('--target-state', required=True)

    rm_p = subparsers.add_parser('rm')
    rm_p.add_argument('--csv', default='migration_plan.csv')

    args = parser.parse_args()
    decoupler = TerraformStateDecoupler(args.state)

    if args.command == 'map':
        if decoupler.load_and_clean_state():
            decoupler.generate_mapping_csv(args.types, args.csv, args.prefix)
    elif args.command == 'move':
        decoupler.execute_moves(args.csv, args.target_state, args.workers)
    elif args.command == 'rm':
        decoupler.execute_rm(args.csv, args.workers)
