import json
import re
from collections import defaultdict
import os

ROOT_DIR = r"c:\Projects\MethodosReconciliationSystem\Data\Reconciliation-Relevant"

def clean_col(c):
    # Remove unnamed pandas columns
    if str(c).lower().startswith("unnamed:"):
        return ""
    # Remove generic numeric indices
    if str(c).isdigit():
        return ""
    # Lowercase and strip whitespace/punctuation
    cleaned = re.sub(r'[^a-zA-Z0-9]', '', str(c).lower())
    return cleaned

def dedupe(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    input_map = defaultdict(lambda: {"files": [], "original_schemas": set(), "display_columns": set()})
    output_map = defaultdict(lambda: {"files": [], "original_schemas": set(), "display_columns": set()})

    def process_map(target_map, sig, files_chunk, schema_key, details):
        target_map[sig]["files"].extend(files_chunk)
        target_map[sig]["original_schemas"].add(schema_key)
        for original in details["columns"]:
            if clean_col(original):
                target_map[sig]["display_columns"].add(str(original).strip())

    for schema_key, details in data.items():
        original_cols = details["columns"]
        
        cleaned_cols = set()
        for col in original_cols:
            c = clean_col(col)
            if c:
                cleaned_cols.add(c)
                
        if not cleaned_cols:
            sig = tuple(["[Malformed or Empty Headers]"])
        else:
            sig = tuple(sorted(list(cleaned_cols)))

        # Split files gracefully
        input_files = []
        output_files = []
        for p in details["all_files"]:
            normalized = p.replace('/', '\\')
            if "\\MRS\\" in normalized or normalized.startswith("MRS\\") or "MRS" in normalized:
                input_files.append(p)
            elif "\\Life cycle report\\" in normalized or normalized.startswith("Life cycle report\\") or "Life cycle report" in normalized:
                output_files.append(p)
            else:
                input_files.append(p)

        if input_files:
            process_map(input_map, sig, input_files, schema_key, details)
        if output_files:
            process_map(output_map, sig, output_files, schema_key, details)

    output_md = os.path.join(ROOT_DIR, "schemas_summary_deduped.md")
    output_json = os.path.join(ROOT_DIR, "schemas_summary_deduped.json")
    
    json_output_data = {"INPUT_MRS": {}, "OUTPUT_Lifecycle": {}}

    with open(output_md, 'w', encoding='utf-8') as f:
        f.write("# Deduped Data Schemas\n\n")
        f.write(f"**Original Unique Schemas (Combined):** {len(data)}\n\n")

        def write_section(title, schema_map, json_key):
            f.write(f"# {title}\n")
            f.write(f"**Unique Schemas in this section:** {len(schema_map)}\n\n")
            sorted_schemas = sorted(schema_map.items(), key=lambda x: len(x[1]["files"]), reverse=True)
            
            for i, (sig, info) in enumerate(sorted_schemas):
                files = list(set(info["files"]))
                files.sort()
                
                schema_name = f"Deduped_Schema_{i + 1}"
                json_output_data[json_key][schema_name] = {
                    "representative_columns": list(info["display_columns"]) if sig[0] != "[Malformed or Empty Headers]" else sig,
                    "file_count": len(files),
                    "example_files": files[:5],
                    "all_files": files
                }
                
                f.write(f"## {title} Schema {i + 1} (Used in {len(files)} files)\n")
                if sig == ("[Malformed or Empty Headers]",):
                    f.write("**Columns:** `[Malformed, Titles, or Empty Headers]`\n\n")
                else:
                    f.write(f"**Representative Columns:** `{', '.join(sorted(list(info['display_columns'])))}`\n\n")
                
                f.write("**Example Files:**\n")
                for p in files[:5]:
                    rel_path = os.path.relpath(p, ROOT_DIR)
                    f.write(f"- `{rel_path}`\n")
                if len(files) > 5:
                    f.write(f"- *(... and {len(files) - 5} more)*\n")
                f.write("\n---\n\n")

        write_section("INPUT (MRS)", input_map, "INPUT_MRS")
        write_section("OUTPUT (Life cycle report)", output_map, "OUTPUT_Lifecycle")

    with open(output_json, 'w', encoding='utf-8') as f:
        json.dump(json_output_data, f, indent=4)
        
    print(f"Separation complete. Input schemas: {len(input_map)}, Output schemas: {len(output_map)}.")

if __name__ == "__main__":
    dedupe(os.path.join(ROOT_DIR, "schemas_summary.json"))
