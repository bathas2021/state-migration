# Terraform Decoupler 🚀

A powerful utility script (`tf_decoupler.py`) designed to safely extract and migrate specific resources from a monolithic Terraform state file into a brand-new workspace. 

## ✨ Features

* **Dynamic Resource Targeting:** Pass any Terraform resource type (e.g., `google_folder`, `google_project`, `google_project_iam_member`) to instantly find and target them.
* **Phase 1: Mapping:** Scans your state file (ignoring JSON corruption) and generates a `migration_plan.csv`. It guesses the new address but allows you to manually review and edit the CSV before generating any final code.
* **Phase 2: Code Generation:** Reads your reviewed CSV and automatically generates an `imports.tf` file for your new workspace.
* **Phase 3: Cleanup Generation:** Generates a `remove_from_old_state.sh` script packed with `terraform state rm` commands. This allows you to safely drop the migrated resources from your monolithic state once they are secured in the new workspace.
* **Dependency Scrubbing:** Available as an optional command-line flag if you need to scrub dependencies during the migration.

---

## 🛠️ How to Execute

```bash

# Step 1: Generate the Mapping CSV
python3 tf_decoupler.py --state default.tfstate map --types google_folder google_cloud_identity_group

# Step 2: Generate Terraform Code & Cleanup Script
python3 tf_decoupler.py --state default.tfstate move --csv migration_plan.csv --target-state "new-terraform.tfstate"

# Step 3: Initialize the New Workspace
cd path/to/your/new-workspace
terraform init

# Step 4: Push the New State to Remote
terraform state push -force /path/to/where/you/ran/the/script/new-terraform.tfstate

# Step 5: Verify the Migration
terraform plan

# ---------------------------------------------------------
# Helpful Command: Analyze State Resources in the monolith
# ---------------------------------------------------------
terraform state list -state="./default.tfstate" | sed 's/\[.*\]//' | rev | cut -d. -f2 | rev | sort | uniq -c | sort -nr
