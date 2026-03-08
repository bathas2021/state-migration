import json
import csv
import os
import argparse
import subprocess

class TerraformStateDecoupler:
    def __init__(self, state_file='default.tfstate'):
        self.state_file = state_file
        self.state_data = None

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
            print("⚠️ Strict JSON parsing failed. Attempting to extract valid JSON and ignore trailing garbage...")
            try:
                decoder = json.JSONDecoder()
                self.state_data, index = decoder.raw_decode(content)
                print(f"✅ Successfully recovered JSON. Ignored {len(content) - index} characters of corrupted data.")
            except Exception as e:
                print(f"❌ FATAL ERROR: Could not parse state file at all. {e}")
                return False
        return True

    def _get_old_address(self, module_prefix, res_type, res_name, instance):
        """Reconstructs the original Terraform address for an instance."""
        address_parts = [module_prefix] if module_prefix else []
        base_addr = f"{res_type}.{res_name}"
        
        if 'index_key' in instance:
            index_key = instance['index_key']
            if isinstance(index_key, str):
                base_addr += f'["{index_key}"]'
            else:
                base_addr += f'[{index_key}]'
                
        address_parts.append(base_addr)
        return ".".join(address_parts)

    def generate_mapping_csv(self, resource_types, output_csv='migration_plan.csv', new_module_prefix='module.core'):
        """Scans state for specific resource types and generates a CSV mapping."""
        if not self.state_data:
            return

        mappings = []
        for resource in self.state_data.get('resources', []):
            res_type = resource.get('type')
            if resource.get('mode') == 'managed' and res_type in resource_types:
                module_prefix = resource.get('module', '')
                res_name = resource.get('name')

                for instance in resource.get('instances', []):
                    old_addr = self._get_old_address(module_prefix, res_type, res_name, instance)
                    
                    # Generate a generic proposed new address
                    index_suffix = ""
                    if 'index_key' in instance:
                        idx = instance['index_key']
                        index_suffix = f'["{idx}"]' if isinstance(idx, str) else f'[{idx}]'
                        
                    new_addr = f"{new_module_prefix}.{res_type}.{res_name}{index_suffix}"

                    mappings.append({
                        "Resource_Type": res_type,
                        "Old_Address": old_addr,
                        "New_Address": new_addr
                    })

        if not mappings:
            print(f"⚠️ No resources found matching types: {', '.join(resource_types)}")
            return

        with open(output_csv, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=["Resource_Type", "Old_Address", "New_Address"])
            writer.writeheader()
            writer.writerows(mappings)

        print(f"✅ Generated '{output_csv}' with {len(mappings)} resources.")
        print("➡️  ACTION REQUIRED: Open the CSV and update the 'New_Address' column before running the move command.")

    def execute_moves(self, csv_file='migration_plan.csv', target_state='new_workspace.tfstate'):
            """Reads the verified CSV and directly executes terraform state mv commands via subprocess."""
            if not os.path.exists(csv_file):
                print(f"❌ Error: '{csv_file}' not found. Run mapping generation first.")
                return

            print(f"🚀 Starting live state migration from '{self.state_file}' to '{target_state}'...\n")
            print("⚡ Backup generation completely disabled for both source and target.")
            mv_count = 0

            with open(csv_file, 'r') as in_f:
                reader = csv.DictReader(in_f)
                for row in reader:
                    old_addr = row['Old_Address'].strip()
                    new_addr = row['New_Address'].strip()

                    if not new_addr:
                        continue

                    print(f"Moving: {old_addr}  ->  {new_addr}")
                    
                    # Construct the command array with BOTH backup flags disabled
                    cmd = [
                        "terraform", "state", "mv",
                    f"-backup={os.devnull}",   
                    f"-backup-out={os.devnull}", 
                        f"-state={self.state_file}",
                        f"-state-out={target_state}",
                        old_addr,
                        new_addr
                    ]
                    
                    # Execute directly in Python
                    result = subprocess.run(cmd, capture_output=True, text=True)
                    
                    if result.returncode == 0:
                        print("  ✅ Success")
                        mv_count += 1
                    else:
                        print("  ❌ Failed!")
                        print(f"     Reason: {result.stderr.strip()}")

            print(f"\n🎉 Done! Successfully moved {mv_count} resources.")

            # --- AUTO-SCRUB DEPENDENCIES AFTER THE MOVE ---
            print(f"\n🧹 Auto-scrubbing stale dependencies from the newly generated state file '{target_state}'...")
            if os.path.exists(target_state):
                try:
                    with open(target_state, 'r') as f:
                        new_state_data = json.load(f)

                    scrubbed_count = 0
                    for resource in new_state_data.get('resources', []):
                        for instance in resource.get('instances', []):
                            if 'dependencies' in instance and instance['dependencies']:
                                instance['dependencies'] = []
                                scrubbed_count += 1

                    with open(target_state, 'w') as f:
                        json.dump(new_state_data, f, indent=2)
                    
                    print(f"  ✅ Erased dependencies from {scrubbed_count} moved resources. The new state is perfectly clean!")
                except Exception as e:
                    print(f"  ❌ Error trying to scrub the new state file: {e}")
            else:
                print("  ⚠️ Target state file not found to scrub (were any resources actually moved?).")
    def execute_rm(self, csv_file='migration_plan.csv'):
        """Reads the verified CSV and directly executes terraform state rm commands on the old addresses."""
        if not os.path.exists(csv_file):
            print(f"❌ Error: '{csv_file}' not found.")
            return

        print(f"🗑️ Starting resource removal from '{self.state_file}'...\n")
        rm_count = 0

        with open(csv_file, 'r') as in_f:
            reader = csv.DictReader(in_f)
            for row in reader:
                old_addr = row['Old_Address'].strip()

                if not old_addr:
                    continue

                print(f"Removing: {old_addr}")
                cmd = [
                    "terraform", "state", "rm",
                    "-backup=-",
                    f"-state={self.state_file}",
                    old_addr
                ]
                
                result = subprocess.run(cmd, capture_output=True, text=True)
                
                if result.returncode == 0:
                    print("  ✅ Success")
                    rm_count += 1
                else:
                    print("  ❌ Failed!")
                    print(f"     Reason: {result.stderr.strip()}")

        print(f"\n🎉 Done! Successfully removed {rm_count} resources from '{self.state_file}'.")

    def scrub_dependencies(self):
        """Scans the state file and permanently removes all explicit dependencies from resources."""
        if not self.load_and_clean_state():
            return

        scrubbed_count = 0
        for resource in self.state_data.get('resources', []):
            for instance in resource.get('instances', []):
                if 'dependencies' in instance and instance['dependencies']:
                    instance['dependencies'] = []
                    scrubbed_count += 1

        with open(self.state_file, 'w') as f:
            json.dump(self.state_data, f, indent=2)
            
        print(f"🧹 Success! Scrubbed explicit dependencies from {scrubbed_count} resource instances in '{self.state_file}'.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Terraform State Migration Tool")
    parser.add_argument('--state', default='default.tfstate', help="Path to the state file you want to operate on")
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')

    # Command: Map
    map_parser = subparsers.add_parser('map', help='Generate a CSV mapping for specific resource types')
    map_parser.add_argument('--types', nargs='+', required=True, help="Resource types to extract")
    map_parser.add_argument('--csv', default='migration_plan.csv', help="Output CSV file name")
    map_parser.add_argument('--prefix', default='module.core', help="Default prefix for the new module addresses")

    # Command: Move
    move_parser = subparsers.add_parser('move', help='Directly execute terraform state mv commands from CSV')
    move_parser.add_argument('--csv', default='migration_plan.csv', help="Input CSV mapping file")
    move_parser.add_argument('--target-state', required=True, help="Path to the destination state file")

    # Command: Rm
    rm_parser = subparsers.add_parser('rm', help='Directly execute terraform state rm commands from CSV')
    rm_parser.add_argument('--csv', default='migration_plan.csv', help="Input CSV mapping file")

    # Command: Scrub
    scrub_parser = subparsers.add_parser('scrub', help='Remove all explicit dependencies from the state file')

    args = parser.parse_args()

    decoupler = TerraformStateDecoupler(args.state)

    if args.command == 'map':
        if decoupler.load_and_clean_state():
            decoupler.generate_mapping_csv(args.types, args.csv, args.prefix)
            
    elif args.command == 'move':
        decoupler.execute_moves(args.csv, args.target_state)

    elif args.command == 'rm':
        decoupler.execute_rm(args.csv)

    elif args.command == 'scrub':
        decoupler.scrub_dependencies()
        
    else:
        parser.print_help()
