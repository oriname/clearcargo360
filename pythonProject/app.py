# app.py  –  Streamlit front-end for the air-cargo cleaning workflow
import io
import base64
from pathlib import Path
import streamlit as st
import pandas as pd
from openpyxl import load_workbook
import yaml
import json
import re
# ------------------------------------------------------------------ #
# Utility helpers (identical logic to the module, trimmed for brevity)
# ------------------------------------------------------------------ #
DATE_FMT = "%d-%B-%Y"
ALIAS = {
    "UR":  "UGD",
    "P4":  "APK",
    "8V":  "AJK",
    "AF":  "AFR",
    "BA":  "BAW",
    "DL":  "DAL",
    "EK":  "UAE",
    "ET":  "ETH",
    "KQ":  "KQA",
    "KL":  "KLM",
    "LH":  "DLH",
    "QR":  "QTR",
    "AT":  "RAM",
    "WB":  "RWD",
    "SA":  "SAA",
    "TK":  "THY",
    "DT":  "DTA",
    "UA":  "UAL",
    "VS":  "VIR",
    "MS":  "MSR",
}


CALL_SIGN_RE = re.compile(r"^([A-Za-z]+[0-9]*|[0-9]+[A-Za-z]*)(?=[-0-9])?")

CALL_RE = re.compile(r"^([A-Za-z]+)(?=[-0-9])")   # leading letters before first digit/hyphen

def extract_prefix(cs: str) -> str | None:
    if not isinstance(cs, str):
        return None
    m = CALL_RE.match(cs.strip())
    return m.group(1).upper() if m else None


def normalise_date(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce", dayfirst=True)
    fmt = parsed.dt.strftime(DATE_FMT).str.upper()
    return fmt.where(parsed.notna(), series)

def load_mapping(uploaded_file) -> dict:
    """
    Accepts a Streamlit UploadedFile (YAML or JSON) and returns
    a plain dict  {CALLSIGN: AIRLINE}.
    """
    suffix = Path(uploaded_file.name).suffix.lower()

    # Read the bytes once
    data = uploaded_file.read()
    uploaded_file.seek(0)          # reset pointer for any future reads

    if suffix in {".yml", ".yaml"}:
        return yaml.safe_load(data)

    # default: JSON (object or array-of-pairs)
    return json.loads(data.decode("utf-8"))

def clean_and_split(df: pd.DataFrame, mapping: dict) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    # --- normalise headers --------------------------------------------------
    df.columns = [c.strip() for c in df.columns]

    call_col, op_col = "CallSign_FlightNo", "OperatorName"
    awb_air, awb_ser, wt = "AWBIssuingAirline", "AWBSerialNumber", "CargoWeight"

    # --- derive prefix & modern prefix --------------------------------------
    df["__LEG"] = df[call_col].apply(extract_prefix)
    df["__MOD"] = df["__LEG"].map(ALIAS).fillna(df["__LEG"])   # map legacy→modern

    # --- rename operator in place -------------------------------------------
    df[op_col] = (
        df["__MOD"].map(mapping)                 # look up airline by modern prefix
        .where(lambda x: x.notna(), df[op_col])  # fallback to existing OperatorName
        .str.upper()
    )

    # --- replace legacy prefix inside the call-sign -------------------------
    def swap(cs: str, old: str, new: str) -> str:
        return cs.replace(old, new, 1) if pd.notna(old) and pd.notna(new) else cs

    df[call_col] = [
        swap(cs, old, new)
        for cs, old, new in zip(df[call_col], df["__LEG"], df["__MOD"])
    ]

    # --- normalise any *DATE* columns ---------------------------------------
    for col in (c for c in df.columns if "DATE" in c.upper()):
        df[col] = normalise_date(df[col])

    # --- capture unmapped prefixes ------------------------------------------
    unmapped = df[df["__MOD"].notna() & ~df["__MOD"].isin(mapping)].copy()

    # --- deduplicate per airline --------------------------------------------
    df["__WB"] = df[awb_air].astype(str) + "-" + df[awb_ser].astype(str)
    cleaned: dict[str, pd.DataFrame] = {}
    for name, grp in df.groupby(op_col, sort=True):
        dedup = grp[~grp.duplicated(["__WB", wt])].copy()
        dedup.drop(columns=["__LEG", "__MOD", "__WB"], inplace=True)
        cleaned[name] = dedup.reset_index(drop=True)

    return cleaned, unmapped



def build_multi_sheet_excel(data: dict[str, pd.DataFrame], unmapped: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as w:
        for name, frame in data.items():
            frame.to_excel(w, sheet_name=name[:31], index=False)
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

def b64_download(data: bytes, filename: str, label: str):
    b64 = base64.b64encode(data).decode()
    href = f'<a href="data:application/octet-stream;base64,{b64}" ' \
           f'download="{filename}">{label}</a>'
    st.markdown(href, unsafe_allow_html=True)

# -----------------------  Streamlit UI  ------------------------------------ #
st.set_page_config(page_title="Air-Cargo Cleaner", layout="wide")
st.title("📦 ClearCargo 360")
st.markdown("#### *From manifest to money — reconciling air-cargo data for accurate billing.*")


raw_file   = st.file_uploader("Raw Data Excel",          type="xlsx")
map_file   = st.file_uploader("Call-sign mapping (YAML or JSON)",    type=("yml", "yaml", "json"))
iata_file  = st.file_uploader("IATA .xlsm template (single sheet)",  type="xlsm")

if st.button("Process", disabled=not (raw_file and map_file and iata_file)):
    try:
        mapping = load_mapping(map_file)
        raw_df  = pd.read_excel(raw_file, sheet_name="MainSheet")
        cleaned, unmapped = clean_and_split(raw_df, mapping)

        # Build outputs
        sheet_book = build_multi_sheet_excel(cleaned, unmapped)
        iata_ready = fill_iata(iata_file.read(), cleaned)

        st.success("Done! Download your files below.")
        if not unmapped.empty:
            st.warning(f"{len(unmapped)} rows have unmapped prefixes. They are in the 'Unmapped' sheet.")
        b64_download(sheet_book, "airline_sheets.xlsx", "⬇️ Airline Sheets (XLSX)")
        b64_download(iata_ready, "iata_ready.xlsm", "⬇️ IATA Template (XLSM)")

    except Exception as e:
        st.error(f"Processing failed: {e}")
