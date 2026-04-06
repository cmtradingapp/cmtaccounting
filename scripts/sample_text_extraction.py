"""
Script 2: Extract text from a sample of PDFs and DOCXs across different PSPs.
Reads the index produced by index_agreements.py.
Output: scripts/output/samples/<PSP>_<filename>.txt
Run: python scripts/sample_text_extraction.py
"""
import json
import random
from pathlib import Path
from collections import defaultdict

INDEX_FILE   = Path(__file__).parent / "output" / "agreements_index.json"
SAMPLES_DIR  = Path(__file__).parent / "output" / "samples"
MAX_CHARS    = 8000   # chars to save per document
PDF_SAMPLE   = 5      # number of PDFs to sample
DOCX_SAMPLE  = 3      # number of DOCXs to sample


def extract_pdf(path: str) -> str:
    import pdfplumber
    text_parts = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            t = page.extract_text()
            if t:
                text_parts.append(t)
    return "\n".join(text_parts)


def extract_docx(path: str) -> str:
    from docx import Document
    doc = Document(path)
    return "\n".join(p.text for p in doc.paragraphs if p.text.strip())


def pick_samples(index, ext, n):
    """Pick n files with given extension, spread across different PSP folders."""
    by_psp = defaultdict(list)
    for entry in index:
        if entry["ext"] == ext and entry["size_kb"] > 5:
            by_psp[entry["psp_folder"]].append(entry)

    selected = []
    psps = list(by_psp.keys())
    random.shuffle(psps)
    for psp in psps:
        candidates = sorted(by_psp[psp], key=lambda x: -x["size_kb"])
        selected.append(candidates[0])
        if len(selected) >= n:
            break
    return selected


def main():
    if not INDEX_FILE.exists():
        print(f"ERROR: Run index_agreements.py first. Missing: {INDEX_FILE}")
        return

    with open(INDEX_FILE, encoding="utf-8") as f:
        index = json.load(f)

    SAMPLES_DIR.mkdir(parents=True, exist_ok=True)

    tasks = [
        ("pdf",  pick_samples(index, "pdf",  PDF_SAMPLE),  extract_pdf),
        ("docx", pick_samples(index, "docx", DOCX_SAMPLE), extract_docx),
    ]

    total_extracted = 0
    print(f"\n{'='*60}")
    print("SAMPLE TEXT EXTRACTION")
    print(f"{'='*60}")

    for ext, samples, extractor in tasks:
        print(f"\n--- {ext.upper()} samples ({len(samples)}) ---")
        for entry in samples:
            safe_psp  = entry["psp_folder"].replace(" ", "_").replace("/", "-")
            safe_name = entry["filename"].replace(" ", "_")
            out_file  = SAMPLES_DIR / f"{safe_psp}__{safe_name}.txt"

            print(f"\n  PSP:  {entry['psp_folder']}")
            print(f"  File: {entry['filename']}  ({entry['size_kb']} KB)")

            try:
                text = extractor(entry["path"])
                truncated = text[:MAX_CHARS]

                with open(out_file, "w", encoding="utf-8") as f:
                    f.write(f"SOURCE: {entry['path']}\n")
                    f.write(f"PSP:    {entry['psp_folder']}\n")
                    f.write(f"SIZE:   {entry['size_kb']} KB\n")
                    f.write("="*60 + "\n\n")
                    f.write(truncated)
                    if len(text) > MAX_CHARS:
                        f.write(f"\n\n[...TRUNCATED — {len(text)} chars total, showing first {MAX_CHARS}]")

                preview = text[:300].strip()[:200].encode("ascii", errors="replace").decode("ascii")
                print(f"  Chars extracted: {len(text):,}  -> saved {min(len(text), MAX_CHARS):,} chars")
                print(f"  Preview: {preview!r}")
                print(f"  Saved to: {out_file.name}")
                total_extracted += 1

            except Exception as e:
                print(f"  ERROR: {e}")

    print(f"\n{'='*60}")
    print(f"Done. {total_extracted} files extracted to: {SAMPLES_DIR}")
    print(f"Review the .txt files to understand agreement structure before tuning the AI prompt.")


if __name__ == "__main__":
    main()
