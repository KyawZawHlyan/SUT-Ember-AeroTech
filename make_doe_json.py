"""
Reads wildfire suppression scenario definitions from an Excel worksheet and
emits a JSON file that the simulation program can consume.

The script enables Design of Experiments (DoE) workflows:
- Analysts define scenarios in a spreadsheet (fleet size per group, suppression
  tactics, optional alternative tactics with change conditions).
- This script converts each row into the toolkit’s nested JSON schema.

Inputs (expected)
-----------------
- Excel path: BASE / "MoE Analysis.xlsx"
- Worksheet: SHEET_NAME (e.g., "Pyrenees DoE")
- Required columns (by convention across sheets):
  * "scenario"                       (row identifier)
  * "first group", "second group"    (aircraft per group; may be 0)
  * Group prefixes: g1_/g2_ (main), g1a_/g2a_ (alternative)
    Fields under each prefix (some may be blank):
      - select_poi, track_poi, suppress
      - change_condition, threshold

Rule for alternative tactics
----------------------------
Emit an "alternative" tactic ONLY when a change condition is provided. If no change
condition is present, output only the "main" tactic for that group.

Output (schema)
---------------
{
  "default_params_file": "<SheetRoot>.json",
  "agents": [
    [   # scenario 1: list of 1–2 groups
      {
        "file_name": "<AIRCRAFT_FILE>",
        "agents_per_base": [n_base0, n_base1, ...],
        "suppression_tactic": {
          "main": {...},
          "alternative": {              # optional
            "change_condition": "...",
            "threshold": <num>,
            "alternative_tactic": {...}
          }
        }
      },
      ...
    ],
    [ ... ],  # scenario 2
    ...
  ]
}

Why this exists
---------------
Hand-writing JSON for many scenarios is error-prone. This script is the bridge
from a user-friendly spreadsheet to a strict, simulator-compatible JSON format.
"""

import pandas as pd
import json
from pathlib import Path
import sys

# === CONFIG ===
# Sheet and file locations. Adjust SHEET_NAME to the target worksheet.
# NUM_BASES: number of bases to distribute aircraft across (left-heavy, near-even).
# AIRCRAFT_FILE: agent JSON file name for all generated entries.
# EXCEL_PATH: source spreadsheet with DoE rows.
# OUTPUT_JSON: derived from the sheet name for traceable outputs.
SHEET_NAME = "Pyrenees DoE"  
NUM_BASES = 2 
AIRCRAFT_FILE = "SUT_series_hybrid.json"
BASE = Path("examples/wildfire/data/doe/gen_input")
EXCEL_PATH = BASE / "MoE Analysis.xlsx"
OUTPUT_JSON = BASE / f"doe_gen_{SHEET_NAME.split()[0].lower()}.json"

if NUM_BASES < 1:
    raise ValueError("Number of bases must be >= 1")
# =============

def distribute_across_bases(total: int, num_bases: int) -> list[int]:
    """
    Split 'total' aircraft across 'num_bases' with a near-even, left-heavy distribution.

    Examples
    --------
    total=5, num_bases=2  -> [3, 2]
    total=1, num_bases=3  -> [1, 0, 0]

    Parameters
    ----------
    total : int
        Total aircraft for a group in a scenario row.
    num_bases : int
        Number of active bases.

    Returns
    -------
    list[int]
        Counts per base (length == num_bases). Surplus goes to earlier bases.
    """
    if total >= num_bases:
        base = total // num_bases
        remainder = total % num_bases
        counts = [base] * num_bases
        for i in range(remainder):
            counts[i] += 1
        return counts
    else:
        return [1] * total + [0] * (num_bases - total)

def safe_str(val):
    """
    Normalize a cell to a clean string or None.

    - Returns None for NaN/empty strings.
    - Strips whitespace.
    - Leaves numeric-like content intact (Excel may store numbers).

    Returns
    -------
    str | None
    """
    if pd.isna(val):
        return None
    s = str(val).strip()
    return s if s != "" else None

def build_suppression(main_prefix: str, alt_prefix: str, row: pd.Series) -> dict:
    """
    Build the suppression tactic block for a group, including an optional alternative.

    Behavior
    --------
    - Reads "main" tactic fields from columns with 'main_prefix' (e.g., "g1_"):
        select_poi, track_poi, suppress (only if present).
    - Emits an "alternative" tactic ONLY when a change condition exists:
        * first checks alt_prefix change_condition; if empty, falls back to main_prefix.
        * if still empty -> DO NOT include 'alternative' in the result.
    - When an alternative is present:
        * alternative_tactic fields are taken from alt_prefix if provided.
        * If a specific alt field is blank, it is simply omitted (no implicit flips).

    Parameters
    ----------
    main_prefix : str
        Column prefix for the primary tactic (e.g., "g1_").
    alt_prefix : str
        Column prefix for the alternative tactic (e.g., "g1a_").
    row : pd.Series
        Current Excel row.

    Returns
    -------
    dict
        {"main": {...}} or {"main": {...}, "alternative": {...}} when change_condition is present.
    """
    main = {}
    if safe_str(row.get(f"{main_prefix}select_poi")):
        main["select_poi"] = safe_str(row.get(f"{main_prefix}select_poi"))
    if safe_str(row.get(f"{main_prefix}track_poi")):
        main["track_poi"] = safe_str(row.get(f"{main_prefix}track_poi"))
    if safe_str(row.get(f"{main_prefix}suppress")):
        main["suppress"] = safe_str(row.get(f"{main_prefix}suppress"))

    tactic = {"main": main}

    alt_fields = [
        safe_str(row.get(f"{alt_prefix}select_poi")),
        safe_str(row.get(f"{alt_prefix}track_poi")),
        safe_str(row.get(f"{alt_prefix}suppress")),
        safe_str(row.get(f"{alt_prefix}change_condition")),
        None if pd.isna(row.get(f"{alt_prefix}threshold", None)) else row.get(f"{alt_prefix}threshold"),
    ]
    main_cond_thresh = any([
        safe_str(row.get(f"{main_prefix}change_condition")),
        None if pd.isna(row.get(f"{main_prefix}threshold", None)) else row.get(f"{main_prefix}threshold"),
    ])
    alt_present = any(alt_fields) or main_cond_thresh

    if alt_present:
        alt_tactic = {}
        if safe_str(row.get(f"{alt_prefix}select_poi")):
            alt_tactic["select_poi"] = safe_str(row.get(f"{alt_prefix}select_poi"))
        else:
            main_sel = main.get("select_poi", "").lower()
            if main_sel == "vegetation":
                alt_tactic["select_poi"] = "water"
            elif main_sel == "water":
                alt_tactic["select_poi"] = "vegetation"
            else:
                alt_tactic["select_poi"] = "vegetation"
        alt_tactic["track_poi"] = safe_str(row.get(f"{alt_prefix}track_poi")) or "follow_firefront"
        alt_tactic["suppress"] = safe_str(row.get(f"{alt_prefix}suppress")) or "direct"

        alt_obj = {}
        change_cond = safe_str(row.get(f"{alt_prefix}change_condition")) or safe_str(row.get(f"{main_prefix}change_condition"))
        if change_cond:
            alt_obj["change_condition"] = change_cond
        thresh = None
        if not pd.isna(row.get(f"{alt_prefix}threshold", None)):
            val = row.get(f"{alt_prefix}threshold")
            thresh = int(val) if isinstance(val, (float, int)) and float(val).is_integer() else val
        elif not pd.isna(row.get(f"{main_prefix}threshold", None)):
            val = row.get(f"{main_prefix}threshold")
            thresh = int(val) if isinstance(val, (float, int)) and float(val).is_integer() else val
        if thresh is not None:
            alt_obj["threshold"] = thresh

        alt_obj["alternative_tactic"] = alt_tactic
        tactic["alternative"] = alt_obj

    return tactic

def main():
    """
    Entry point:
      - Load Excel sheet (SHEET_NAME) from EXCEL_PATH.
      - Forward-fill columns to carry down grouped values.
      - Convert each valid row into 1–2 group entries (first/second group).
      - Write a JSON file compatible with the simulator.

    Iteration rules
    ---------------
    - Skip rows without a valid 'scenario' id.
    - For each group with count > 0:
        * Distribute aircraft across NUM_BASES (near-even, left-heavy).
        * Build suppression_tactic via build_suppression().
        * Append to the scenario's list.
    - JSON top-level keys:
        * default_params_file: "<SheetRoot>.json"
        * agents: list of scenarios (each scenario is a list of group dicts)

    Output is written to OUTPUT_JSON and a summary is printed to stdout.
    """
    if not EXCEL_PATH.exists():
        print(f"Excel file not found at {EXCEL_PATH.resolve()}", file=sys.stderr)
        return

    try:
        df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME, header=0)
    except Exception as e:
        print(f"Failed to open sheet '{SHEET_NAME}' in {EXCEL_PATH}: {e}", file=sys.stderr)
        return

    df.ffill(inplace=True)
    # Forward-fill: many sheets carry top-row values down a block; ffill keeps
    # group context even when intermediate cells are blank in the spreadsheet.


    scenarios = []

    # Build scenarios row-by-row. Each non-empty "scenario" id yields one scenario
    # (which contains 1–2 group entries, depending on first/second group counts).
    for _, row in df.iterrows():
        scen = row.get("scenario")
        if pd.isna(scen) or str(scen).strip() == "":
            continue  # skip blank/invalid rows
                # Skip formatting rows or spacers without a scenario id.

        scenario_entries = []

        # ----- Group 1 (first group) -----
        # If "first group" > 0, emit an entry using g1_* (and g1a_* if change_condition exists).
        first_count = int(row.get("first group", 0)) if pd.notna(row.get("first group")) else 0
        if first_count > 0:
            apb = distribute_across_bases(first_count, NUM_BASES)
            suppression = build_suppression("g1_", "g1a_", row)
            scenario_entries.append({
                "file_name": AIRCRAFT_FILE,
                "agents_per_base": apb,
                "suppression_tactic": suppression
            })

        # ----- Group 2 (second group) -----
        # If "second group" > 0, emit an entry using g2_* (and g2a_* if change_condition exists).
        second_count = int(row.get("second group", 0)) if pd.notna(row.get("second group")) else 0
        if second_count > 0:
            apb = distribute_across_bases(second_count, NUM_BASES)
            suppression = build_suppression("g2_", "g2a_", row)
            scenario_entries.append({
                "file_name": AIRCRAFT_FILE,
                "agents_per_base": apb,
                "suppression_tactic": suppression
            })

        if scenario_entries:
            scenarios.append(scenario_entries)

    # Assemble top-level JSON. <SheetRoot>.json lets the simulator pick default params
    # for the region (e.g., "Pyrenees.json").
    output = {
        "default_params_file": f"{SHEET_NAME.split()[0]}.json",
        "agents": scenarios,
    }

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)
        # Pretty-print for reviewability and diff-friendly version control.
    print(f"Wrote {OUTPUT_JSON} with {len(scenarios)} scenario entries.")

if __name__ == "__main__":
    main()
