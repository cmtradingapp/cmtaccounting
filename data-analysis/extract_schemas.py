import os
import csv
import json
import logging
from collections import defaultdict

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

ROOT_DIR = r"c:\Projects\MethodosReconciliationSystem\Data\Reconciliation-Relevant"
TARGET_DIRS = ["MRS", "Life cycle report"]

def extract_csv_columns(filepath):
    encodings = ['utf-8', 'utf-8-sig', 'cp1252', 'iso-8859-1']
    for enc in encodings:
        try:
            with open(filepath, 'r', encoding=enc) as f:
                first_line = f.readline().strip()
                if not first_line:
                    return None
                
                # Attempt to determine dialect using sniffing or simple character logic
                try:
                    sniffer = csv.Sniffer()
                    # Feed a couple of lines if possible
                    sample = first_line + '\n' + f.readline() + '\n' + f.readline()
                    dialect = sniffer.sniff(sample)
                except csv.Error:
                    if ',' in first_line: 
                        dialect = csv.excel
                    elif ';' in first_line: 
                        dialect = csv.excel
                        dialect.delimiter = ';'
                    elif '\t' in first_line: 
                        dialect = csv.excel_tab
                    else: 
                        return [first_line]
                
                f.seek(0)
                reader = csv.reader(f, dialect)
                for row in reader:
                    return row
        except UnicodeDecodeError:
            continue
        except Exception as e:
            logging.error(f"Error parsing CSV {filepath} with encoding {enc}: {e}")
            break
    return None

def extract_excel_columns(filepath):
    try:
        import pandas as pd
        df = pd.read_excel(filepath, nrows=0)
        return list(df.columns)
    except ImportError:
        logging.error("pandas and openpyxl/xlrd are required to parse Excel files.")
        raise
    except Exception as e:
        error_msg = str(e).lower()
        if "cannot be determined" in error_msg or "zip" in error_msg:
            try:
                import pandas as pd
                # Many systems export HTML tables with an .xls extension
                tables = pd.read_html(filepath, keep_default_na=False)
                if tables:
                    return list(tables[0].columns)
            except Exception as e2:
                # Could be a TSV or CSV renamed to .xls
                csv_cols = extract_csv_columns(filepath)
                # Ensure it's not just returning the first line of an HTML file
                if csv_cols and not any("<html" in str(c).lower() or "<xml" in str(c).lower() for c in csv_cols):
                    return csv_cols
                logging.error(f"Fallback parsing HTML/CSV failed for {filepath}: {e} | {e2}")
                return None
        logging.error(f"Error parsing Excel {filepath}: {e}")
        return None

def main():
    schema_map = defaultdict(list)
    
    total_files = 0
    processed_files = 0
    errors = 0

    try:
        import pandas as pd # test early to fail gracefully before long runs
    except ImportError:
        print("Please run: pip install pandas openpyxl xlrd")
        return

    for target in TARGET_DIRS:
        target_path = os.path.join(ROOT_DIR, target)
        if not os.path.exists(target_path):
            logging.warning(f"Directory {target_path} not found.")
            continue
            
        for root, _, files in os.walk(target_path):
            for file in files:
                ext = file.lower().split('.')[-1]
                if ext not in ['csv', 'xlsx', 'xls']:
                    continue
                    
                total_files += 1
                filepath = os.path.join(root, file)
                
                columns = None
                if ext == 'csv':
                    columns = extract_csv_columns(filepath)
                elif ext in ['xlsx', 'xls']:
                    columns = extract_excel_columns(filepath)
                
                if columns:
                    # Clean up column names (strip and handle potential nan from pandas)
                    try:
                        columns = [str(c).strip() for c in columns if str(c) != 'nan']
                    except Exception:
                        columns = [str(c).strip() for c in columns]
                    # Create a tuple signature for the schema
                    schema_signature = tuple(columns)
                    schema_map[schema_signature].append(filepath)
                    processed_files += 1
                else:
                    errors += 1

                if total_files % 100 == 0:
                    logging.info(f"Scanned {total_files} files so far...")

    # Output to JSON
    output_json = os.path.join(ROOT_DIR, "schemas_summary.json")
    json_data = {}
    for i, (schema_sig, paths) in enumerate(schema_map.items()):
        schema_name = f"Schema_{i + 1}"
        json_data[schema_name] = {
            "columns": list(schema_sig),
            "file_count": len(paths),
            "example_files": paths[:5],
            "all_files": paths
        }
        
    with open(output_json, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, indent=4)
        
    # Output to Markdown
    output_md = os.path.join(ROOT_DIR, "schemas_summary.md")
    with open(output_md, 'w', encoding='utf-8') as f:
        f.write("# Unique Data Schemas Found\n\n")
        f.write(f"**Total Files Scanned:** {total_files}\n")
        f.write(f"**Successfully Processed:** {processed_files}\n")
        f.write(f"**Errors/Empty Headers:** {errors}\n")
        f.write(f"**Unique Schemas Found:** {len(schema_map)}\n\n")
        
        sorted_schemas = sorted(schema_map.items(), key=lambda x: len(x[1]), reverse=True)
        
        for i, (schema_sig, paths) in enumerate(sorted_schemas):
            f.write(f"## Schema {i + 1} (Used in {len(paths)} files)\n")
            if schema_sig:
                f.write(f"**Columns:** `{', '.join(schema_sig)}`\n\n")
            else:
                f.write("**Columns:** `[Empty/Unnamed Columns]`\n\n")
            f.write("**Example Files:**\n")
            for p in paths[:3]:
                rel_path = os.path.relpath(p, ROOT_DIR)
                f.write(f"- `{rel_path}`\n")
            if len(paths) > 3:
                f.write(f"- *(... and {len(paths) - 3} more)*\n")
            f.write("\n---\n\n")
            
    logging.info(f"Done. Found {len(schema_map)} unique schemas. Summaries saved to JSON and MD.")

if __name__ == "__main__":
    main()
