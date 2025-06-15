# app.py  –  Streamlit front-end for the air-cargo cleaning workflow
import streamlit as st

# MUST be the very first Streamlit command
st.set_page_config(page_title="ClearCargo360", layout="wide")

# Now import everything else
import io
import base64
from pathlib import Path
import pandas as pd
from openpyxl import load_workbook
import yaml
import json
import re

# ------------------------------------------------------------------ #
# Utility helpers (FIXED VERSION)
# ------------------------------------------------------------------ #
DATE_FMT = "%d-%b-%Y"

# FIXED: Better regex to capture airline prefixes from call signs
CALL_SIGN_RE = re.compile(r"^([A-Za-z]+[0-9]*|[0-9]+[A-Za-z]*)(?=[-0-9])?")

# FIXED: Updated regex to capture common airline prefix patterns
CALL_RE = re.compile(r"^([A-Za-z]{1,3}|[0-9]{1,2}[A-Za-z]{1,2}|[A-Za-z]{1,2}[0-9]{1,2})")


def extract_prefix(cs: str) -> str | None:
    """Extract airline prefix from call sign with improved pattern matching"""
    if not isinstance(cs, str):
        return None
    cs = cs.strip().upper()

    # Handle common patterns for airline prefixes:
    # UR-900, UR0900 -> UR (keep original call sign)
    # P47579 -> P4, QY081 -> QY, ME571 -> ME
    # AFR123 -> AFR, BCS456 -> BCS

    # First, try to find letter sequences at the start
    # Look for 2-3 letters followed by numbers, hyphens, or end of string
    match = re.match(r'^([A-Za-z]{2,3})(?=[-0-9]|$)', cs)
    if match:
        return match.group(1).upper()

    # If no match, try 2 characters (handles cases like P4, 8V)
    if len(cs) >= 2:
        prefix = cs[:2]
        # Accept if it's letters, or letter+digit, or digit+letter
        if (prefix.isalpha() or
                (prefix[0].isalpha() and prefix[1].isdigit()) or
                (prefix[0].isdigit() and prefix[1].isalpha())):
            return prefix.upper()

    return None


def handle_numeric_callsigns(df: pd.DataFrame) -> pd.DataFrame:
    """Handle call signs that start with numbers by prepending OperatorAccountingCode"""
    call_col = "CallSign_FlightNo"
    acc_code_col = "OperatorAccountingCode"

    # Check if OperatorAccountingCode column exists
    if acc_code_col not in df.columns:
        return df

    # Find rows where call sign starts with a number (like 1407A, 123B, etc.)
    starts_with_number_mask = df[call_col].str.match(r'^[0-9]', na=False)

    # For call signs starting with numbers, prepend the accounting code
    df.loc[starts_with_number_mask, call_col] = (
            df.loc[starts_with_number_mask, acc_code_col].astype(str) +
            df.loc[starts_with_number_mask, call_col].astype(str)
    )

    return df


def normalise_date(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce", dayfirst=True)
    fmt = parsed.dt.strftime(DATE_FMT).str.upper()
    return fmt.where(parsed.notna(), series)


def load_mapping_and_alias(uploaded_file) -> tuple[dict, dict]:
    """
    Accept a JSON/YAML file that EITHER:
      • contains {'mapping': {...}, 'alias': {...}}
      • OR is a flat {prefix: airline} object (then alias = {})
    Returns (modern_map, alias_map)
    """
    suffix = Path(uploaded_file.name).suffix.lower()
    data = yaml.safe_load(uploaded_file) if suffix in {".yml", ".yaml"} \
        else json.loads(uploaded_file.read().decode("utf-8"))
    uploaded_file.seek(0)  # reset for any future reads

    if "mapping" in data:
        return data["mapping"], data.get("alias", {})
    return data, {}  # simple flat file


def clean_and_split(df: pd.DataFrame, mapping: dict, alias: dict) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    # --- normalise headers --------------------------------------------------
    df.columns = [c.strip() for c in df.columns]

    call_col, op_col = "CallSign_FlightNo", "OperatorName"
    awb_air, awb_ser, wt = "AWBIssuingAirline", "AWBSerialNumber", "CargoWeight"

    # --- handle numeric-only call signs first -------------------------------
    df = handle_numeric_callsigns(df)

    # --- derive prefix & modern prefix --------------------------------------
    df["__LEG"] = df[call_col].apply(extract_prefix)
    df["__MOD"] = df["__LEG"].map(alias).fillna(df["__LEG"])  # map legacy→modern using alias

    # --- rename operator ONLY (do NOT change call sign) --------------------
    df[op_col] = (
        df["__MOD"].map(mapping)  # look up airline by modern prefix
        .where(lambda x: x.notna(), df[op_col])  # fallback to existing OperatorName
        .str.upper()
    )

    # --- DO NOT replace legacy prefix in call-sign! ------------------------
    # REMOVED: The call sign swapping logic that was changing UR900 to UGD900
    # Call signs should remain unchanged: UR900 stays UR900

    # --- normalise any *DATE* columns ---------------------------------------
    for col in (c for c in df.columns if "DATE" in c.upper()):
        df[col] = normalise_date(df[col])

    # --- additional operator name cleaning -----------------------------------
    # Handle common variations that might not be caught by prefix mapping
    operator_fixes = {
        'QATAR': 'QATAR AIRWAYS',
        'EMIRATES AIRLINES': 'EMIRATES AIRLINE',
        'LUFTHANSA GERMAN AIRLINES': 'DEUTSCHE LUFTHANSA AG',
        'DELTA AIRLINE': 'DELTA AIR LINES INC',
        'KLM ROYAL': 'KLM ROYAL DUTCH AIRLINES',
        'EGYPT': 'EGYPTAIR',
        'DHL INTERNATIONAL NIGERIA LIMITED': 'EUROPEAN AIR TRANSPORT',
        'AIRPEACE': 'AIR PEACE LIMITED',  # Standardize Air Peace variations
        'AIR PEACE': 'AIR PEACE LIMITED'
    }

    # Apply exact matches only
    for old_name, new_name in operator_fixes.items():
        df[op_col] = df[op_col].str.replace(old_name, new_name, regex=False)

    # --- capture unmapped prefixes ------------------------------------------
    # FIXED: Only include rows where the modern prefix is NOT in the mapping
    unmapped = df[
        df["__MOD"].notna() &
        ~df["__MOD"].isin(mapping) &
        df["__LEG"].notna()  # Only include rows with valid extracted prefixes
        ].copy()

    # --- deduplicate per airline --------------------------------------------
    df["__WB"] = df[awb_air].astype(str) + "-" + df[awb_ser].astype(str)
    cleaned: dict[str, pd.DataFrame] = {}

    # Only process rows that have valid mappings
    mapped_df = df[df["__MOD"].isin(mapping)].copy()

    for name, grp in mapped_df.groupby(op_col, sort=True):
        dedup = grp[~grp.duplicated(["__WB", wt])].copy()
        dedup.drop(columns=["__LEG", "__MOD", "__WB"], inplace=True)
        cleaned[name] = dedup.reset_index(drop=True)

    return cleaned, unmapped


def build_multi_sheet_excel(data: dict[str, pd.DataFrame], unmapped: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as w:
        # Create combined sheet first with all data
        combined_data = []
        for airline_name, frame in data.items():
            combined_data.append(frame)

        if combined_data:
            combined_df = pd.concat(combined_data, ignore_index=True)
            combined_df.to_excel(w, sheet_name="Combined", index=False)

        # Create individual airline sheets
        for name, frame in data.items():
            frame.to_excel(w, sheet_name=name[:31], index=False)

        # Add unmapped sheet if there are unmapped rows
        if not unmapped.empty:
            unmapped.to_excel(w, sheet_name="Unmapped", index=False)

    return buffer.getvalue()


def fill_iata(template_bytes: bytes, cleaned: dict[str, pd.DataFrame],
              sheet: str = "CARGO SALES") -> bytes:
    buffer = io.BytesIO(template_bytes)
    wb = load_workbook(buffer, keep_vba=True)
    ws = wb[sheet]
    if ws.max_row > 1:
        ws.delete_rows(2, ws.max_row - 1)
    for frame in cleaned.values():
        for row in frame.itertuples(index=False):
            ws.append(list(row))
    out_buf = io.BytesIO()
    wb.save(out_buf)
    return out_buf.getvalue()


def create_duplicate_report(dups1: pd.DataFrame, dups2: pd.DataFrame,
                            file1_name: str, file2_name: str, comparison_method: str) -> bytes:
    """Create a detailed Excel report of duplicate records between two files"""
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as w:
        # Summary sheet
        summary_data = {
            'Metric': [
                'File 1 Name', 'File 2 Name', 'Comparison Method',
                'Duplicates in File 1', 'Duplicates in File 2', 'Analysis Date'
            ],
            'Value': [
                file1_name, file2_name, comparison_method,
                len(dups1), len(dups2), pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
            ]
        }
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(w, sheet_name="Summary", index=False)

        # Duplicates from file 1
        if not dups1.empty:
            dups1.to_excel(w, sheet_name=f"Duplicates_from_{file1_name[:15]}", index=False)

        # Duplicates from file 2
        if not dups2.empty:
            dups2.to_excel(w, sheet_name=f"Duplicates_from_{file2_name[:15]}", index=False)

    return buffer.getvalue()


def b64_download(data: bytes, filename: str, label: str):
    b64 = base64.b64encode(data).decode()
    href = f'<a href="data:application/octet-stream;base64,{b64}" ' \
           f'download="{filename}">{label}</a>'
    st.markdown(href, unsafe_allow_html=True)


# -----------------------  Streamlit UI  ------------------------------------ #
st.title("📦 ClearCargo 360")
st.markdown("#### *From manifest to money — reconciling air-cargo data for accurate billing.*")

# Create tabs for different functions
tab1, tab2 = st.tabs(["🔧 Reconcile Data", "🔍 Billing Conflict Finder"])

with tab1:
    st.markdown("### Clean and Process Air Cargo Data")

    raw_file = st.file_uploader("Raw Data Excel", type="xlsx", key="clean_raw")
    map_file = st.file_uploader("Call-sign mapping (YAML or JSON)", type=("yml", "yaml", "json"), key="clean_map")
    iata_file = st.file_uploader("IATA .xlsm template (single sheet)", type="xlsm", key="clean_iata")

    if st.button("Process", disabled=not (raw_file and map_file and iata_file)):
        try:
            # FIXED: Properly unpack both mapping and alias
            mapping, alias = load_mapping_and_alias(map_file)
            raw_df = pd.read_excel(raw_file, sheet_name="MainSheet")

            # FIXED: Pass both mapping and alias to the function
            cleaned, unmapped = clean_and_split(raw_df, mapping, alias)

            # Build outputs
            sheet_book = build_multi_sheet_excel(cleaned, unmapped)
            iata_ready = fill_iata(iata_file.read(), cleaned)

            st.success("Done! Download your files below.")

            # Display some debug info
            st.write("**Processing Summary:**")
            st.write(f"- Total airlines processed: {len(cleaned)}")
            st.write(f"- Airlines: {', '.join(cleaned.keys())}")

            # Calculate total rows in combined data
            total_rows = sum(len(df) for df in cleaned.values())
            st.write(f"- Total rows in combined sheet: {total_rows}")

            # Debug: Check specific call signs
            call_col = "CallSign_FlightNo"  # Define it here for debugging
            if 'UR' in [extract_prefix(cs) for cs in raw_df[call_col].dropna()[:100]]:
                ur_rows = raw_df[raw_df[call_col].str.contains('UR', na=False)]
                if not ur_rows.empty:
                    st.write("**🔍 UR Call Signs Found:**")
                    ur_sample = ur_rows[[call_col, "OperatorName"]].head(5)
                    st.dataframe(ur_sample)

            if not unmapped.empty:
                unique_unmapped = unmapped['__LEG'].unique()
                st.warning(f"{len(unmapped)} rows have unmapped prefixes: {', '.join(unique_unmapped)}")

                # Show sample unmapped for debugging
                st.write("**Sample Unmapped Rows:**")
                unmapped_sample = unmapped[["CallSign_FlightNo", "OperatorName", '__LEG', '__MOD']].head(5)
                st.dataframe(unmapped_sample)
            else:
                st.success("✅ No unmapped data found!")

            b64_download(sheet_book, "airline_sheets.xlsx", "⬇️ Airline Sheets (XLSX)")
            b64_download(iata_ready, "iata_ready.xlsm", "⬇️ IATA Template (XLSM)")

        except Exception as e:
            st.error(f"Processing failed: {e}")
            st.exception(e)  # Show full traceback for debugging

with tab2:
    st.markdown("### Check for Duplicates Between Two Files")
    st.markdown("*Compare two Excel files to find duplicate records based on AWB (waybill) numbers and weights.*")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**📅 First Period (e.g., June 1-7)**")
        file1 = st.file_uploader("First Excel file", type="xlsx", key="dup_file1")
        if file1:
            sheet1 = st.selectbox("Select sheet from first file",
                                  options=pd.ExcelFile(file1).sheet_names,
                                  key="sheet1")

    with col2:
        st.markdown("**📅 Second Period (e.g., June 8-17)**")
        file2 = st.file_uploader("Second Excel file", type="xlsx", key="dup_file2")
        if file2:
            sheet2 = st.selectbox("Select sheet from second file",
                                  options=pd.ExcelFile(file2).sheet_names,
                                  key="sheet2")

    # Duplicate checking options
    st.markdown("**🔧 Duplicate Detection Settings**")
    check_weight = st.checkbox("Include weight in duplicate check", value=True,
                               help="If checked, rows must have same AWB AND weight to be considered duplicates")

    if st.button("🔍 Check for Duplicates", disabled=not (file1 and file2)):
        try:
            # Read the selected sheets
            df1 = pd.read_excel(file1, sheet_name=sheet1)
            df2 = pd.read_excel(file2, sheet_name=sheet2)

            # Normalize column names
            df1.columns = [c.strip() for c in df1.columns]
            df2.columns = [c.strip() for c in df2.columns]

            # Check if required columns exist
            required_cols = ["AWBIssuingAirline", "AWBSerialNumber"]
            if check_weight:
                required_cols.append("CargoWeight")

            missing_cols_1 = [col for col in required_cols if col not in df1.columns]
            missing_cols_2 = [col for col in required_cols if col not in df2.columns]

            if missing_cols_1 or missing_cols_2:
                st.error(f"Missing columns - File 1: {missing_cols_1}, File 2: {missing_cols_2}")
                st.info(
                    "Required columns: AWBIssuingAirline, AWBSerialNumber" + (", CargoWeight" if check_weight else ""))
            else:
                # Create composite keys for comparison
                awb_air, awb_ser, wt = "AWBIssuingAirline", "AWBSerialNumber", "CargoWeight"

                if check_weight:
                    df1["__KEY"] = df1[awb_air].astype(str) + "-" + df1[awb_ser].astype(str) + "-" + df1[wt].astype(str)
                    df2["__KEY"] = df2[awb_air].astype(str) + "-" + df2[awb_ser].astype(str) + "-" + df2[wt].astype(str)
                    key_description = "AWB + Weight"
                else:
                    df1["__KEY"] = df1[awb_air].astype(str) + "-" + df1[awb_ser].astype(str)
                    df2["__KEY"] = df2[awb_air].astype(str) + "-" + df2[awb_ser].astype(str)
                    key_description = "AWB Only"

                # Find duplicates
                duplicates_in_2 = df2[df2["__KEY"].isin(df1["__KEY"])].copy()
                duplicates_in_1 = df1[df1["__KEY"].isin(df2["__KEY"])].copy()

                # Remove the temporary key column
                duplicates_in_1.drop(columns=["__KEY"], inplace=True)
                duplicates_in_2.drop(columns=["__KEY"], inplace=True)

                # Display results
                st.markdown("---")
                st.markdown("### 📊 Duplicate Analysis Results")

                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("File 1 Total Rows", len(df1))
                with col2:
                    st.metric("File 2 Total Rows", len(df2))
                with col3:
                    st.metric("Duplicate Pairs Found", len(duplicates_in_1))

                st.info(f"**Comparison Method:** {key_description}")

                if len(duplicates_in_1) > 0:
                    st.warning(f"⚠️ Found {len(duplicates_in_1)} duplicate records!")

                    # Show sample duplicates
                    st.markdown("**🔍 Sample Duplicates (from File 1):**")
                    display_cols = [awb_air, awb_ser]
                    if check_weight:
                        display_cols.append(wt)
                    if "OperatorName" in duplicates_in_1.columns:
                        display_cols.append("OperatorName")

                    st.dataframe(duplicates_in_1[display_cols].head(10))

                    # Create downloadable report
                    duplicate_report = create_duplicate_report(duplicates_in_1, duplicates_in_2, file1.name, file2.name,
                                                               key_description)

                    b64_download(duplicate_report, f"duplicate_report_{file1.name}_{file2.name}.xlsx",
                                 "⬇️ Download Full Duplicate Report")

                else:
                    st.success("✅ No duplicates found between the two files!")

        except Exception as e:
            st.error(f"Duplicate check failed: {e}")
            st.exception(e)