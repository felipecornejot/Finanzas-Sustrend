# app.py
# Sustrend — Modelo Financiero 2026 (Streamlit)
# Dashboard dinámico recalculado desde inputs del Excel
# ----------------------------------------------------

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


# =========================
# Config & UI helpers
# =========================

st.set_page_config(
    page_title="Sustrend — Modelo Financiero 2026",
    page_icon="📊",
    layout="wide",
)

DEFAULT_EXCHANGE_RATE = 950.0  # CLP por 1 USD (ajustable en sidebar)

CSS = """
<style>
/* Layout spacing */
.block-container { padding-top: 1.2rem; padding-bottom: 2rem; }

/* Make header-ish feel cleaner */
header[data-testid="stHeader"] { background: rgba(0,0,0,0); }

/* Slightly nicer metric cards spacing */
div[data-testid="stMetric"] { padding: 0.2rem 0.6rem; border-radius: 0.75rem; }

/* Dataframe font */
div[data-testid="stDataFrame"] * { font-size: 0.92rem; }
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)


# =========================
# Core types
# =========================

@dataclass(frozen=True)
class Scenario:
    name: str
    # Multipliers
    mult_precio: float
    mult_personas: float
    mult_opex_fijo: float
    mult_opex_variable: float
    mult_costo_directo: float
    # DSO defaults (days)
    dso_privado: int
    dso_publico_lt10m: int
    dso_publico_gte10m: int
    # Prob conversion by project status
    prob_by_estado: Dict[str, float]


@dataclass(frozen=True)
class Params:
    anio: int
    tasa_iva: float
    saldo_inicial_caja: float
    umbral_publico_10m: float
    iva_remanente_inicial: float


# =========================
# Excel load & parsing
# =========================

@st.cache_data(show_spinner=False)
def read_sheet(excel_bytes: bytes, sheet_name: str, header: Optional[int] = None) -> pd.DataFrame:
    """Read an Excel sheet from uploaded bytes with optional header row."""
    return pd.read_excel(excel_bytes, sheet_name=sheet_name, header=header, engine="openpyxl")


@st.cache_data(show_spinner=False)
def list_sheets(excel_bytes: bytes) -> List[str]:
    xls = pd.ExcelFile(excel_bytes, engine="openpyxl")
    return xls.sheet_names


def ensure_month_end(x) -> pd.Timestamp:
    """Force a date-like to month-end Timestamp."""
    ts = pd.to_datetime(x)
    if pd.isna(ts):
        return ts
    # Normalize to month end
    return (ts + pd.offsets.MonthEnd(0)).normalize()


def month_end_add(month_end: pd.Timestamp, months: int) -> pd.Timestamp:
    """Add N months to a month-end date, returning month-end."""
    if pd.isna(month_end):
        return month_end
    return (month_end + pd.offsets.MonthEnd(months)).normalize()


def ceil_div_days_to_months(dso_days: float) -> int:
    """Convert DSO days to months shift using ceil(dso/30). Minimum 0."""
    if pd.isna(dso_days):
        return 0
    d = float(dso_days)
    if d <= 0:
        return 0
    return int(math.ceil(d / 30.0))


def melt_month_matrix(df: pd.DataFrame, id_cols: List[str], value_name: str = "Monto") -> pd.DataFrame:
    """
    Melt a sheet that has month-end columns as datetimes and a Total_Año col.
    Keeps only month columns (Timestamp-like).
    """
    month_cols = []
    for c in df.columns:
        if c == "Total_Año":
            continue
        if isinstance(c, (pd.Timestamp, np.datetime64)):
            month_cols.append(c)
        else:
            # sometimes openpyxl reads as datetime.datetime
            try:
                if pd.to_datetime(c, errors="raise") is not None and str(c).startswith("202"):
                    # fallback
                    month_cols.append(c)
            except Exception:
                pass

    # If month cols are strings like "2026-01-31 00:00:00"
    cleaned_month_cols = []
    for c in month_cols:
        try:
            ts = ensure_month_end(c)
            cleaned_month_cols.append((c, ts))
        except Exception:
            continue

    keep_cols = id_cols + [orig for orig, _ in cleaned_month_cols]
    tmp = df[keep_cols].copy()

    melted = tmp.melt(id_vars=id_cols, var_name="Mes_fin_raw", value_name=value_name)
    melted["Mes_fin"] = melted["Mes_fin_raw"].apply(ensure_month_end)
    melted.drop(columns=["Mes_fin_raw"], inplace=True)
    return melted


def parse_params(excel_bytes: bytes) -> Params:
    raw = read_sheet(excel_bytes, "02_PARAMETROS", header=None)
    # Header row is 2: [Nombre, Valor, Descripción]
    tbl = raw.iloc[2:].copy()
    tbl.columns = ["Nombre", "Valor", "Descripcion"]
    tbl = tbl.dropna(subset=["Nombre"]).copy()
    tbl["Nombre"] = tbl["Nombre"].astype(str)

    def getv(name: str, default=None):
        s = tbl.loc[tbl["Nombre"] == name, "Valor"]
        if len(s) == 0:
            return default
        v = s.iloc[0]
        return default if pd.isna(v) else v

    return Params(
        anio=int(getv("ANIO", 2026)),
        tasa_iva=float(getv("TASA_IVA", 0.19)),
        saldo_inicial_caja=float(getv("SALDO_INICIAL_CAJA", 0.0)),
        umbral_publico_10m=float(getv("UMBRAL_PUBLICO_10M", 10_000_000)),
        iva_remanente_inicial=float(getv("IVA_REMANENTE_INICIAL", 0.0)),
    )


def parse_scenarios(excel_bytes: bytes) -> Dict[str, Scenario]:
    raw = read_sheet(excel_bytes, "02B_ESCENARIOS", header=None)

    # Section A: parameter table begins at row where col0 == "Parámetro"
    # Format: Parámetro | Base | Conservador | Agresivo
    a_start = raw.index[raw.iloc[:, 0].astype(str) == "Parámetro"].tolist()
    if not a_start:
        raise ValueError("No se encontró la tabla 'Parámetro' en 02B_ESCENARIOS.")
    a0 = a_start[0]
    a = raw.iloc[a0:].copy()
    a.columns = ["Parametro", "Base", "Conservador", "Agresivo"] + list(a.columns[4:])
    a = a[["Parametro", "Base", "Conservador", "Agresivo"]]
    a = a.dropna(subset=["Parametro"]).copy()
    # stop at the header of the next section "Estado"
    stop_idx = a.index[a["Parametro"].astype(str) == "Estado"].tolist()
    if stop_idx:
        a = a.loc[: stop_idx[0] - 1].copy()

    # Section B: probabilities table begins at row where col0 == "Estado"
    b_start = raw.index[raw.iloc[:, 0].astype(str) == "Estado"].tolist()
    if not b_start:
        raise ValueError("No se encontró la tabla 'Estado' (probabilidades) en 02B_ESCENARIOS.")
    b0 = b_start[0]
    b = raw.iloc[b0:].copy()
    b.columns = ["Estado", "Base", "Conservador", "Agresivo"] + list(b.columns[4:])
    b = b[["Estado", "Base", "Conservador", "Agresivo"]].dropna(subset=["Estado"]).copy()

    # Helpers
    def get_param(df: pd.DataFrame, name: str, scen: str, default=None):
        s = df.loc[df["Parametro"].astype(str) == name, scen]
        if len(s) == 0:
            return default
        v = s.iloc[0]
        return default if pd.isna(v) else v

    def prob_map(df: pd.DataFrame, scen: str) -> Dict[str, float]:
        out = {}
        for _, r in df.iterrows():
            estado = str(r["Estado"]).strip()
            out[estado] = float(r[scen])
        return out

    scenarios = {}
    for scen in ["Base", "Conservador", "Agresivo"]:
        scenarios[scen] = Scenario(
            name=scen,
            dso_privado=int(get_param(a, "DSO_PRIVADO_DEFAULT", scen, 30)),
            dso_publico_lt10m=int(get_param(a, "DSO_PUBLICO_LT10M_DEFAULT", scen, 60)),
            dso_publico_gte10m=int(get_param(a, "DSO_PUBLICO_GTE10M_DEFAULT", scen, 60)),
            mult_precio=float(get_param(a, "MULT_PRECIO", scen, 1.0)),
            mult_personas=float(get_param(a, "MULT_PERSONAS", scen, 1.0)),
            mult_opex_fijo=float(get_param(a, "MULT_OPEX_FIJO", scen, 1.0)),
            mult_opex_variable=float(get_param(a, "MULT_OPEX_VARIABLE", scen, 1.0)),
            mult_costo_directo=float(get_param(a, "MULT_COSTO_DIRECTO", scen, 1.0)),
            prob_by_estado=prob_map(b, scen),
        )

    return scenarios


def load_inputs(excel_bytes: bytes) -> Dict[str, pd.DataFrame]:
    """
    Load all required input sheets with correct header rows.
    We intentionally recompute outputs from inputs (since output sheets may have formulas not cached).
    """
    # header=2 for most "Inputs" tables in this workbook
    df = {}
    df["cal"] = read_sheet(excel_bytes, "01_CALENDARIO", header=0).copy()

    df["proyectos"] = read_sheet(excel_bytes, "04_PROYECTOS", header=2).copy()
    df["hitos"] = read_sheet(excel_bytes, "04B_HITOS", header=2).copy()

    df["personas"] = read_sheet(excel_bytes, "05_PERSONAS", header=2).copy()

    df["opex"] = read_sheet(excel_bytes, "06_OPEX", header=2).copy()
    df["opex_custom"] = read_sheet(excel_bytes, "06C_OPEX_CUSTOM", header=2).copy()

    df["cd_meta"] = read_sheet(excel_bytes, "06D_COSTOS_DIRECTOS", header=2).copy()
    df["cd_mensual"] = read_sheet(excel_bytes, "06E_COSTOS_DIRECTOS_MENSUAL", header=2).copy()

    df["noop_mensual"] = read_sheet(excel_bytes, "07B_NO_OPERACIONALES_MENSUAL", header=2).copy()

    return df


# =========================
# Normalize / clean inputs
# =========================

def clean_calendar(df_cal: pd.DataFrame) -> pd.DataFrame:
    out = df_cal.copy()
    out["Mes_fin"] = out["Mes_fin"].apply(ensure_month_end)
    out["Mes_idx"] = out["Mes_idx"].astype(int)
    return out.sort_values("Mes_fin")


def clean_projects(df_proj: pd.DataFrame, params: Params) -> pd.DataFrame:
    out = df_proj.copy()
    # Dates
    for c in ["Mes_inicio", "Mes_fin"]:
        out[c] = out[c].apply(ensure_month_end)

    # IVA_tasa fallback
    out["IVA_tasa"] = out["IVA_tasa"].fillna(params.tasa_iva).astype(float)

    # Normalize types
    out["Tipo_cliente"] = out["Tipo_cliente"].astype(str).str.strip()
    out["Estado"] = out["Estado"].astype(str).str.strip()

    # Ensure numeric
    out["Monto_neto_CLP"] = pd.to_numeric(out["Monto_neto_CLP"], errors="coerce").fillna(0.0)

    # Keep a clean customer field
    out["Cliente"] = out["Cliente"].astype(str).str.strip()

    return out


def clean_hitos(df_hitos: pd.DataFrame) -> pd.DataFrame:
    out = df_hitos.copy()
    out["Mes_facturacion"] = out["Mes_facturacion"].apply(ensure_month_end)
    out["Pct_facturacion"] = pd.to_numeric(out["Pct_facturacion"], errors="coerce").fillna(0.0)
    out["Glosa"] = out.get("Glosa", "").astype(str)
    # DSO override at hit-level (optional)
    if "DSO_dias" in out.columns:
        out["DSO_dias"] = pd.to_numeric(out["DSO_dias"], errors="coerce")
    return out


def clean_personas(df_personas: pd.DataFrame) -> pd.DataFrame:
    out = df_personas.copy()
    for c in ["Mes_inicio", "Mes_fin", "Bono_mes_1", "Bono_mes_2"]:
        if c in out.columns:
            out[c] = out[c].apply(ensure_month_end)
    for c in ["Monto_base_mensual", "Previred_pct", "Reajuste_pct_anual", "Monto_bono_1", "Monto_bono_2"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0.0)
    out["Centro_costo"] = out.get("Centro_costo", "N/A").astype(str).str.strip()
    out["Tipo_vinculo"] = out.get("Tipo_vinculo", "N/A").astype(str).str.strip()
    return out


def clean_opex(df_opex: pd.DataFrame) -> pd.DataFrame:
    out = df_opex.copy()
    for c in ["Mes_inicio", "Mes_fin", "Mes_unico"]:
        if c in out.columns:
            out[c] = out[c].apply(ensure_month_end)
    out["Monto_neto"] = pd.to_numeric(out["Monto_neto"], errors="coerce").fillna(0.0)
    out["IVA_afecto"] = out["IVA_afecto"].astype(str).str.strip().str.upper()
    out["Tipo_gasto"] = out["Tipo_gasto"].astype(str).str.strip()
    out["Recurrencia"] = out["Recurrencia"].astype(str).str.strip()
    out["Centro_costo"] = out.get("Centro_costo", "N/A").astype(str).str.strip()
    return out


def clean_cd(df_cd_meta: pd.DataFrame, df_cd_m: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    meta = df_cd_meta.copy()
    meta["IVA_afecto"] = meta["IVA_afecto"].astype(str).str.strip().str.upper()
    meta["Centro_costo"] = meta.get("Centro_costo", "N/A").astype(str).str.strip()

    mens = df_cd_m.copy()
    # remove TOTAL_CD and blank rows
    if "CD_ID" in mens.columns:
        mens = mens[mens["CD_ID"].notna()].copy()
        mens = mens[mens["CD_ID"].astype(str).str.upper() != "TOTAL_CD"].copy()
    return meta, mens


def clean_noop(df_noop: pd.DataFrame) -> pd.DataFrame:
    out = df_noop.copy()
    out["Tipo"] = out["Tipo"].astype(str).str.strip()
    return out


# =========================
# Build facts
# =========================

def resolve_project_dso_days(row_proj: pd.Series, scenario: Scenario, params: Params) -> int:
    """
    Determine DSO days for a project:
    1) If project has DSO_dias_override, use it
    2) Else based on Tipo_cliente and scenario defaults
    """
    if "DSO_dias_override" in row_proj and not pd.isna(row_proj["DSO_dias_override"]):
        try:
            return int(row_proj["DSO_dias_override"])
        except Exception:
            pass

    tipo = str(row_proj.get("Tipo_cliente", "")).strip()
    # Types in this workbook: Privado, Publico_<10M, Publico_>=10M
    if tipo == "Privado":
        return scenario.dso_privado
    if tipo == "Publico_<10M":
        return scenario.dso_publico_lt10m
    if tipo == "Publico_>=10M":
        return scenario.dso_publico_gte10m

    # fallback: treat unknown as privado
    return scenario.dso_privado


def prob_from_estado(estado: str, scenario: Scenario) -> float:
    e = str(estado).strip()
    # default to 1 if not found
    return float(scenario.prob_by_estado.get(e, 1.0))


def build_fact_revenue_devengo(
    cal: pd.DataFrame,
    proyectos: pd.DataFrame,
    hitos: pd.DataFrame,
    scenario: Scenario,
) -> pd.DataFrame:
    """
    Revenue devengo by project-hito-month.
    Uses: 04_PROYECTOS + 04B_HITOS
    """
    p = proyectos.copy()
    h = hitos.copy()

    base = h.merge(p, on="Proyecto_ID", how="left", suffixes=("", "_proj"))

    # probability by Estado
    base["Prob_conv"] = base["Estado"].apply(lambda x: prob_from_estado(x, scenario))

    # compute net per hito (devengo)
    base["Monto_neto_hito"] = (
        base["Monto_neto_CLP"]
        * base["Pct_facturacion"]
        * scenario.mult_precio
        * base["Prob_conv"]
    )

    base["IVA_hito"] = base["Monto_neto_hito"] * base["IVA_tasa"]
    base["Monto_bruto_hito"] = base["Monto_neto_hito"] + base["IVA_hito"]

    # Keep only months in calendar
    months = set(cal["Mes_fin"].tolist())
    base = base[base["Mes_facturacion"].isin(months)].copy()

    # DSO at project level (can be overridden)
    base["DSO_dias_eff"] = base.apply(lambda r: resolve_project_dso_days(r, scenario, None), axis=1)  # params not needed here

    # hit-level optional override: if 04B_HITOS has DSO_dias numeric, use it
    if "DSO_dias" in base.columns:
        base["DSO_dias_eff"] = np.where(
            base["DSO_dias"].notna(),
            base["DSO_dias"].astype(float),
            base["DSO_dias_eff"].astype(float),
        )

    base["Shift_meses"] = base["DSO_dias_eff"].apply(ceil_div_days_to_months).astype(int)
    base["Mes_cobro"] = base.apply(lambda r: month_end_add(r["Mes_facturacion"], int(r["Shift_meses"])), axis=1)

    # Only keep fields needed for drill-down & aggregation
    keep = [
        "Proyecto_ID", "Proyecto_nombre", "Cliente", "Tipo_cliente", "Estado",
        "Mes_facturacion", "Hito_n", "Glosa", "Pct_facturacion",
        "Monto_neto_hito", "IVA_hito", "Monto_bruto_hito",
        "DSO_dias_eff", "Shift_meses", "Mes_cobro",
    ]
    return base[keep].copy()


def build_fact_cobranzas(fact_rev: pd.DataFrame, cal: pd.DataFrame) -> pd.DataFrame:
    """Aggregate cobranzas (cash-in) by month from revenue fact and Mes_cobro."""
    months = set(cal["Mes_fin"].tolist())
    tmp = fact_rev.copy()
    tmp = tmp[tmp["Mes_cobro"].isin(months)].copy()
    out = tmp.groupby("Mes_cobro", as_index=False)["Monto_bruto_hito"].sum()
    out.rename(columns={"Mes_cobro": "Mes_fin", "Monto_bruto_hito": "Cobros_brutos"}, inplace=True)
    return out


def build_fact_opex_monthly(
    cal: pd.DataFrame,
    opex: pd.DataFrame,
    opex_custom: pd.DataFrame,
    scenario: Scenario,
    params: Params,
) -> pd.DataFrame:
    """
    Expand OPEX by month:
    - Recurrencia Mensual: apply each month between Mes_inicio..Mes_fin
    - Recurrencia Unico: apply on Mes_unico (or Mes_inicio)
    - Recurrencia Custom: from 06C_OPEX_CUSTOM (matrix) melted by month
    """
    months = cal["Mes_fin"].tolist()
    month_set = set(months)

    base_rows = []

    # Mensual / Unico
    for _, r in opex.iterrows():
        rid = r["OPEX_ID"]
        rec = str(r["Recurrencia"])
        tipo_gasto = str(r["Tipo_gasto"])
        centro = str(r.get("Centro_costo", "N/A"))
        iva_af = str(r["IVA_afecto"]).upper() == "SI"
        monto = float(r["Monto_neto"])

        # multiplier by Tipo_gasto
        mult = scenario.mult_opex_fijo if tipo_gasto.lower() == "fijo" else scenario.mult_opex_variable
        monto_eff = monto * mult

        if rec == "Mensual":
            start = r["Mes_inicio"]
            end = r["Mes_fin"]
            if pd.isna(start) or pd.isna(end):
                continue
            # iterate all calendar months and include those in range
            for m in months:
                if m >= start and m <= end:
                    base_rows.append((rid, r["Concepto"], tipo_gasto, centro, iva_af, m, monto_eff))
        elif rec == "Unico":
            m = r["Mes_unico"] if not pd.isna(r.get("Mes_unico", np.nan)) else r["Mes_inicio"]
            if pd.isna(m):
                continue
            if m in month_set:
                base_rows.append((rid, r["Concepto"], tipo_gasto, centro, iva_af, m, monto_eff))
        elif rec == "Custom":
            # handled later via opex_custom
            pass

    base = pd.DataFrame(
        base_rows,
        columns=["OPEX_ID", "Concepto", "Tipo_gasto", "Centro_costo", "IVA_afecto_bool", "Mes_fin", "OPEX_neto"],
    )

    # Custom matrix
    custom = opex_custom.copy()
    if len(custom) > 0:
        # the custom sheet has OPEX_ID and Concepto
        id_cols = ["OPEX_ID", "Concepto"]
        m = melt_month_matrix(custom, id_cols=id_cols, value_name="OPEX_neto_raw")
        m["OPEX_neto_raw"] = pd.to_numeric(m["OPEX_neto_raw"], errors="coerce").fillna(0.0)

        # join metadata from opex sheet
        meta = opex[["OPEX_ID", "Tipo_gasto", "Centro_costo", "IVA_afecto"]].copy()
        meta["IVA_afecto_bool"] = meta["IVA_afecto"].astype(str).str.upper().eq("SI")

        m = m.merge(meta, on="OPEX_ID", how="left")
        m["Tipo_gasto"] = m["Tipo_gasto"].fillna("Fijo")
        m["Centro_costo"] = m["Centro_costo"].fillna("N/A")

        # apply multipliers by type
        m["mult"] = np.where(m["Tipo_gasto"].str.lower() == "fijo", scenario.mult_opex_fijo, scenario.mult_opex_variable)
        m["OPEX_neto"] = m["OPEX_neto_raw"] * m["mult"]
        m = m[["OPEX_ID", "Concepto", "Tipo_gasto", "Centro_costo", "IVA_afecto_bool", "Mes_fin", "OPEX_neto"]].copy()

        base = pd.concat([base, m], ignore_index=True)

    # Filter to calendar months
    base = base[base["Mes_fin"].isin(month_set)].copy()

    # VAT credit
    base["IVA_credito_opex"] = np.where(base["IVA_afecto_bool"], base["OPEX_neto"] * params.tasa_iva, 0.0)
    base["OPEX_bruto"] = base["OPEX_neto"] + base["IVA_credito_opex"]

    return base


def build_fact_costos_directos_monthly(
    cal: pd.DataFrame,
    cd_meta: pd.DataFrame,
    cd_mensual: pd.DataFrame,
    scenario: Scenario,
    params: Params,
) -> pd.DataFrame:
    """
    Build monthly direct costs from 06E matrix and 06D metadata.
    Notes:
    - In this file, monthly values appear negative (eg -5,057,017). We normalize to positive cost magnitude.
    """
    id_cols = ["CD_ID", "Concepto"]
    m = melt_month_matrix(cd_mensual, id_cols=id_cols, value_name="CD_neto_raw")
    m["CD_neto_raw"] = pd.to_numeric(m["CD_neto_raw"], errors="coerce").fillna(0.0)

    # Normalize sign: treat costs as positive magnitude
    m["CD_neto_raw"] = m["CD_neto_raw"].abs()

    # Merge IVA_afecto & centro
    meta = cd_meta[["CD_ID", "IVA_afecto", "Centro_costo"]].copy()
    meta["IVA_afecto_bool"] = meta["IVA_afecto"].astype(str).str.upper().eq("SI")

    m = m.merge(meta, on="CD_ID", how="left")
    m["IVA_afecto_bool"] = m["IVA_afecto_bool"].fillna(True)
    m["Centro_costo"] = m["Centro_costo"].fillna("N/A")

    # Apply scenario multiplier
    m["CD_neto"] = m["CD_neto_raw"] * scenario.mult_costo_directo

    # VAT credit
    m["IVA_credito_cd"] = np.where(m["IVA_afecto_bool"], m["CD_neto"] * params.tasa_iva, 0.0)
    m["CD_bruto"] = m["CD_neto"] + m["IVA_credito_cd"]

    # calendar filter
    month_set = set(cal["Mes_fin"].tolist())
    m = m[m["Mes_fin"].isin(month_set)].copy()

    return m[["CD_ID", "Concepto", "Centro_costo", "Mes_fin", "CD_neto", "IVA_credito_cd", "CD_bruto"]].copy()


def build_fact_personas_monthly(
    cal: pd.DataFrame,
    personas: pd.DataFrame,
    scenario: Scenario,
) -> pd.DataFrame:
    """
    Expand people costs by month from 05_PERSONAS:
    - Base monthly
    - Bonuses at Bono_mes_1 / Bono_mes_2
    - Previred_pct applies to (base+bonuses) in that month
    """
    months = cal["Mes_fin"].tolist()
    rows = []

    for _, r in personas.iterrows():
        pid = r["Persona_ID"]
        nombre = r["Nombre"]
        rol = r["Rol"]
        centro = r.get("Centro_costo", "N/A")
        start = r["Mes_inicio"]
        end = r["Mes_fin"]

        base = float(r["Monto_base_mensual"]) * scenario.mult_personas
        prev_pct = float(r["Previred_pct"])

        bono1_mes = r.get("Bono_mes_1", pd.NaT)
        bono1_val = float(r.get("Monto_bono_1", 0.0)) * scenario.mult_personas
        bono2_mes = r.get("Bono_mes_2", pd.NaT)
        bono2_val = float(r.get("Monto_bono_2", 0.0)) * scenario.mult_personas

        for m in months:
            if pd.isna(start) or pd.isna(end):
                continue
            if not (m >= start and m <= end):
                continue

            bonos = 0.0
            if not pd.isna(bono1_mes) and m == bono1_mes:
                bonos += bono1_val
            if not pd.isna(bono2_mes) and m == bono2_mes:
                bonos += bono2_val

            pago = base + bonos
            prev = pago * prev_pct
            total = pago + prev

            rows.append((pid, nombre, rol, centro, m, pago, prev, total))

    out = pd.DataFrame(
        rows,
        columns=["Persona_ID", "Nombre", "Rol", "Centro_costo", "Mes_fin", "Pago", "Previred", "Total_personas"],
    )
    return out


def build_fact_no_operacionales_monthly(
    cal: pd.DataFrame,
    noop_mensual: pd.DataFrame,
) -> pd.DataFrame:
    """
    Melt no-operational monthly sheet 07B:
    - Tipo == 'Ingreso' => positive
    - else => negative
    """
    id_cols = ["NOP_ID", "Concepto", "Tipo"]
    m = melt_month_matrix(noop_mensual, id_cols=id_cols, value_name="NOP_raw")
    m["NOP_raw"] = pd.to_numeric(m["NOP_raw"], errors="coerce").fillna(0.0)
    m["sign"] = np.where(m["Tipo"].astype(str).str.lower().str.contains("ingreso"), 1.0, -1.0)
    m["NOP_monto"] = m["NOP_raw"] * m["sign"]

    month_set = set(cal["Mes_fin"].tolist())
    m = m[m["Mes_fin"].isin(month_set)].copy()

    return m[["NOP_ID", "Concepto", "Tipo", "Mes_fin", "NOP_monto"]].copy()


# =========================
# IVA, P&L, Cash, KPIs
# =========================

def build_iva_ledger(
    cal: pd.DataFrame,
    fact_rev: pd.DataFrame,
    fact_opex: pd.DataFrame,
    fact_cd: pd.DataFrame,
    params: Params,
) -> pd.DataFrame:
    """
    IVA model:
    - Debit: IVA from sales at invoice month (Mes_facturacion)
    - Credit: IVA from purchases (opex + direct costs) at purchase month
    - Net IVA: debit - credit
    - Remanente carried; payment occurs the *next* month-end for positive payable
    """
    months = cal["Mes_fin"].tolist()

    # IVA debito by month
    iva_deb = (
        fact_rev.groupby("Mes_facturacion", as_index=False)["IVA_hito"].sum()
        .rename(columns={"Mes_facturacion": "Mes_fin", "IVA_hito": "IVA_debito"})
    )

    # IVA credito by month (opex + cd)
    iva_cred_opex = fact_opex.groupby("Mes_fin", as_index=False)["IVA_credito_opex"].sum()
    iva_cred_cd = fact_cd.groupby("Mes_fin", as_index=False)["IVA_credito_cd"].sum()
    iva_cred = iva_cred_opex.merge(iva_cred_cd, on="Mes_fin", how="outer").fillna(0.0)
    iva_cred["IVA_credito"] = iva_cred["IVA_credito_opex"] + iva_cred["IVA_credito_cd"]
    iva_cred = iva_cred[["Mes_fin", "IVA_credito"]]

    # join to calendar
    iva = pd.DataFrame({"Mes_fin": months})
    iva = iva.merge(iva_deb, on="Mes_fin", how="left").merge(iva_cred, on="Mes_fin", how="left")
    iva[["IVA_debito", "IVA_credito"]] = iva[["IVA_debito", "IVA_credito"]].fillna(0.0)

    iva["IVA_neto_mes"] = iva["IVA_debito"] - iva["IVA_credito"]

    # Carry remanente and compute payable
    rem = params.iva_remanente_inicial
    payable_by_month = []
    rem_by_month = []

    for _, r in iva.iterrows():
        net = float(r["IVA_neto_mes"])
        saldo = net + rem
        pay = max(saldo, 0.0)
        rem = min(saldo, 0.0)  # negative means credit carried
        payable_by_month.append(pay)
        rem_by_month.append(rem)

    iva["IVA_por_pagar_mes"] = payable_by_month
    iva["IVA_remanente_fin_mes"] = rem_by_month

    # Payment occurs next month
    iva["IVA_pago_mes_siguiente"] = iva["IVA_por_pagar_mes"].shift(1).fillna(0.0)

    return iva


def build_pyg_monthly(
    cal: pd.DataFrame,
    fact_rev: pd.DataFrame,
    fact_cd: pd.DataFrame,
    fact_opex: pd.DataFrame,
    fact_personas: pd.DataFrame,
) -> pd.DataFrame:
    months = cal["Mes_fin"].tolist()
    pyg = pd.DataFrame({"Mes_fin": months})

    ventas = fact_rev.groupby("Mes_facturacion", as_index=False)["Monto_neto_hito"].sum().rename(
        columns={"Mes_facturacion": "Mes_fin", "Monto_neto_hito": "Ventas_netas"}
    )
    cd = fact_cd.groupby("Mes_fin", as_index=False)["CD_neto"].sum().rename(columns={"CD_neto": "Costos_directos"})
    opex = fact_opex.groupby("Mes_fin", as_index=False)["OPEX_neto"].sum().rename(columns={"OPEX_neto": "OPEX"})
    ppl = fact_personas.groupby("Mes_fin", as_index=False)["Total_personas"].sum().rename(
        columns={"Total_personas": "Personas"}
    )

    pyg = pyg.merge(ventas, on="Mes_fin", how="left").merge(cd, on="Mes_fin", how="left").merge(
        opex, on="Mes_fin", how="left"
    ).merge(ppl, on="Mes_fin", how="left")

    pyg[["Ventas_netas", "Costos_directos", "OPEX", "Personas"]] = pyg[
        ["Ventas_netas", "Costos_directos", "OPEX", "Personas"]
    ].fillna(0.0)

    pyg["Margen_bruto"] = pyg["Ventas_netas"] - pyg["Costos_directos"]
    pyg["Margen_bruto_pct"] = np.where(pyg["Ventas_netas"] != 0, pyg["Margen_bruto"] / pyg["Ventas_netas"], np.nan)

    pyg["EBITDA"] = pyg["Margen_bruto"] - pyg["OPEX"] - pyg["Personas"]
    pyg["EBITDA_pct"] = np.where(pyg["Ventas_netas"] != 0, pyg["EBITDA"] / pyg["Ventas_netas"], np.nan)

    return pyg


def build_cash_flow(
    cal: pd.DataFrame,
    params: Params,
    cobranzas: pd.DataFrame,
    fact_opex: pd.DataFrame,
    fact_cd: pd.DataFrame,
    fact_personas: pd.DataFrame,
    iva: pd.DataFrame,
    noop: pd.DataFrame,
) -> pd.DataFrame:
    months = cal["Mes_fin"].tolist()
    cf = pd.DataFrame({"Mes_fin": months})

    cob = cobranzas.rename(columns={"Cobros_brutos": "Cobros"})
    cf = cf.merge(cob, on="Mes_fin", how="left")
    cf["Cobros"] = cf["Cobros"].fillna(0.0)

    pagos_opex = fact_opex.groupby("Mes_fin", as_index=False)["OPEX_bruto"].sum().rename(columns={"OPEX_bruto": "Pagos_OPEX"})
    pagos_cd = fact_cd.groupby("Mes_fin", as_index=False)["CD_bruto"].sum().rename(columns={"CD_bruto": "Pagos_CD"})
    pagos_ppl = fact_personas.groupby("Mes_fin", as_index=False)["Total_personas"].sum().rename(columns={"Total_personas": "Pagos_Personas"})
    noop_m = noop.groupby("Mes_fin", as_index=False)["NOP_monto"].sum().rename(columns={"NOP_monto": "NoOp_Neto"})

    cf = cf.merge(pagos_opex, on="Mes_fin", how="left").merge(pagos_cd, on="Mes_fin", how="left").merge(
        pagos_ppl, on="Mes_fin", how="left"
    ).merge(noop_m, on="Mes_fin", how="left").merge(
        iva[["Mes_fin", "IVA_pago_mes_siguiente"]], on="Mes_fin", how="left"
    )

    cf[["Pagos_OPEX", "Pagos_CD", "Pagos_Personas", "NoOp_Neto", "IVA_pago_mes_siguiente"]] = cf[
        ["Pagos_OPEX", "Pagos_CD", "Pagos_Personas", "NoOp_Neto", "IVA_pago_mes_siguiente"]
    ].fillna(0.0)

    cf.rename(columns={"IVA_pago_mes_siguiente": "Pago_IVA"}, inplace=True)

    cf["Flujo_neto"] = (
        cf["Cobros"]
        + cf["NoOp_Neto"]
        - cf["Pagos_OPEX"]
        - cf["Pagos_CD"]
        - cf["Pagos_Personas"]
        - cf["Pago_IVA"]
    )

    # Accumulate cash
    saldo = params.saldo_inicial_caja
    saldos = []
    for _, r in cf.iterrows():
        saldo += float(r["Flujo_neto"])
        saldos.append(saldo)

    cf["Caja_fin_mes"] = saldos
    cf["Caja_ini_mes"] = [params.saldo_inicial_caja] + saldos[:-1]

    return cf


def build_kpis(
    pyg: pd.DataFrame,
    cash: pd.DataFrame,
    fact_rev: pd.DataFrame,
) -> Dict[str, float]:
    ventas = float(pyg["Ventas_netas"].sum())
    margen = float(pyg["Margen_bruto"].sum())
    ebitda = float(pyg["EBITDA"].sum())

    caja_min = float(cash["Caja_fin_mes"].min())
    caja_fin = float(cash["Caja_fin_mes"].iloc[-1])

    # Burn monthly: only months with negative flujo
    neg = cash.loc[cash["Flujo_neto"] < 0, "Flujo_neto"]
    burn_prom = float(-neg.mean()) if len(neg) else 0.0

    # runway: months until cash <= 0 using average burn
    runway = float("inf") if burn_prom <= 0 else max(0.0, cash_fin := cash["Caja_fin_mes"].iloc[-1]) / burn_prom

    # concentration Top 3 customers (by net revenue)
    by_cli = fact_rev.groupby("Cliente", as_index=False)["Monto_neto_hito"].sum().sort_values("Monto_neto_hito", ascending=False)
    top3 = float(by_cli.head(3)["Monto_neto_hito"].sum())
    conc_top3 = (top3 / ventas) if ventas > 0 else np.nan

    return {
        "Ventas_netas": ventas,
        "Margen_bruto": margen,
        "EBITDA": ebitda,
        "Margen_pct": (margen / ventas) if ventas else np.nan,
        "EBITDA_pct": (ebitda / ventas) if ventas else np.nan,
        "Caja_min": caja_min,
        "Caja_fin": caja_fin,
        "Burn_prom": burn_prom,
        "Runway_meses_aprox": runway,
        "Conc_top3": conc_top3,
    }


# =========================
# App state & build pipeline
# =========================

@st.cache_data(show_spinner=False)
def compute_model(excel_bytes: bytes, scenario_name: str) -> Dict[str, pd.DataFrame]:
    params = parse_params(excel_bytes)
    scenarios = parse_scenarios(excel_bytes)
    scenario = scenarios[scenario_name]

    inputs = load_inputs(excel_bytes)

    cal = clean_calendar(inputs["cal"])
    proyectos = clean_projects(inputs["proyectos"], params)
    hitos = clean_hitos(inputs["hitos"])
    personas = clean_personas(inputs["personas"])
    opex = clean_opex(inputs["opex"])
    opex_custom = inputs["opex_custom"].copy()
    cd_meta, cd_mens = clean_cd(inputs["cd_meta"], inputs["cd_mensual"])
    noop = clean_noop(inputs["noop_mensual"])

    fact_rev = build_fact_revenue_devengo(cal, proyectos, hitos, scenario)
    cobranzas = build_fact_cobranzas(fact_rev, cal)

    fact_opex = build_fact_opex_monthly(cal, opex, opex_custom, scenario, params)
    fact_cd = build_fact_costos_directos_monthly(cal, cd_meta, cd_mens, scenario, params)
    fact_ppl = build_fact_personas_monthly(cal, personas, scenario)
    fact_noop = build_fact_no_operacionales_monthly(cal, noop)

    iva = build_iva_ledger(cal, fact_rev, fact_opex, fact_cd, params)
    pyg = build_pyg_monthly(cal, fact_rev, fact_cd, fact_opex, fact_ppl)
    cash = build_cash_flow(cal, params, cobranzas, fact_opex, fact_cd, fact_ppl, iva, fact_noop)

    return {
        "cal": cal,
        "params": pd.DataFrame([params.__dict__]),
        "scenario": pd.DataFrame([{
            "name": scenario.name,
            "mult_precio": scenario.mult_precio,
            "mult_personas": scenario.mult_personas,
            "mult_opex_fijo": scenario.mult_opex_fijo,
            "mult_opex_variable": scenario.mult_opex_variable,
            "mult_costo_directo": scenario.mult_costo_directo,
            "dso_privado": scenario.dso_privado,
            "dso_publico_lt10m": scenario.dso_publico_lt10m,
            "dso_publico_gte10m": scenario.dso_publico_gte10m,
        }]),
        "fact_rev": fact_rev,
        "cobranzas": cobranzas,
        "fact_opex": fact_opex,
        "fact_cd": fact_cd,
        "fact_ppl": fact_ppl,
        "fact_noop": fact_noop,
        "iva": iva,
        "pyg": pyg,
        "cash": cash,
    }


# =========================
# Validation checks
# =========================

def run_checks(model: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Basic inadmissibility-like checks for modeling consistency:
    - Hitos sum to 1 per project
    - Projects missing hitos
    - Months outside calendar
    - Negative or missing critical fields
    """
    cal = model["cal"]
    months = set(cal["Mes_fin"].tolist())

    rev = model["fact_rev"].copy()

    issues = []

    # Hitos sum per project
    hit_sum = rev.groupby("Proyecto_ID", as_index=False)["Pct_facturacion"].sum()
    bad = hit_sum[~np.isclose(hit_sum["Pct_facturacion"], 1.0)]
    for _, r in bad.iterrows():
        issues.append(("HITOS_PCT_NE_1", r["Proyecto_ID"], f"Suma de hitos != 1 (={r['Pct_facturacion']:.3f})"))

    # Mes_facturacion outside calendar
    if not rev["Mes_facturacion"].isin(months).all():
        bad_rows = rev[~rev["Mes_facturacion"].isin(months)][["Proyecto_ID", "Mes_facturacion"]].drop_duplicates()
        for _, r in bad_rows.iterrows():
            issues.append(("MES_FACT_FUERA_CAL", r["Proyecto_ID"], f"Mes_facturacion fuera calendario: {r['Mes_facturacion']}"))

    # Missing client or name
    miss_cli = rev[(rev["Cliente"].isna()) | (rev["Cliente"].astype(str).str.strip() == "")]
    if len(miss_cli):
        ids = miss_cli["Proyecto_ID"].unique().tolist()
        for pid in ids:
            issues.append(("PROY_SIN_CLIENTE", pid, "Proyecto sin cliente (campo Cliente vacío)."))

    # Zero revenue but has hitos
    by_proj = rev.groupby("Proyecto_ID", as_index=False)["Monto_neto_hito"].sum()
    zero = by_proj[by_proj["Monto_neto_hito"] <= 0]
    for _, r in zero.iterrows():
        issues.append(("PROY_INGRESO_CERO", r["Proyecto_ID"], "Ingreso neto total <= 0 (probabilidad/importe/hitos)."))

    return pd.DataFrame(issues, columns=["Codigo", "Entidad", "Detalle"])


# =========================
# Formatting helpers
# =========================

def fmt_money(x: float) -> str:
    try:
        return f"${x:,.0f}".replace(",", ".")
    except Exception:
        return str(x)


def fmt_pct(x: float) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "—"
    return f"{x*100:.1f}%"


def to_currency(df: pd.DataFrame, cols: List[str], mode: str, fx: float) -> pd.DataFrame:
    """
    Convert monetary cols to USD if mode == 'USD'.
    Keeps raw numeric, but conversion occurs on displayed copy.
    """
    out = df.copy()
    if mode == "USD":
        for c in cols:
            if c in out.columns:
                out[c] = out[c] / fx
    return out


# =========================
# UI: Sidebar
# =========================

st.title("📊 Sustrend — Modelo Financiero 2026 (dinámico)")

with st.sidebar:
    st.subheader("Datos")
    uploaded = st.file_uploader("Sube el Excel (.xlsx)", type=["xlsx"])

    if uploaded is None:
        st.info("Sube el archivo para correr el dashboard.")
        st.stop()

    excel_bytes = uploaded.getvalue()

    st.subheader("Escenario")
    scenarios = parse_scenarios(excel_bytes)
    scen = st.selectbox("Escenario", options=list(scenarios.keys()), index=0)

    st.subheader("Conversión (opcional)")
    currency = st.radio("Moneda", ["CLP", "USD"], horizontal=True)
    fx = st.number_input("Tipo de cambio (CLP por 1 USD)", min_value=1.0, value=float(DEFAULT_EXCHANGE_RATE), step=1.0)

    st.subheader("Filtros")
    # We'll build filters after model is computed (needs customers/projects list)


# =========================
# Compute model
# =========================

with st.spinner("Calculando modelo desde inputs..."):
    model = compute_model(excel_bytes, scen)

params_row = model["params"].iloc[0].to_dict()
params = Params(**params_row)

fact_rev = model["fact_rev"]
pyg = model["pyg"]
cash = model["cash"]
iva = model["iva"]
fact_opex = model["fact_opex"]
fact_cd = model["fact_cd"]
fact_ppl = model["fact_ppl"]
fact_noop = model["fact_noop"]


# =========================
# Build dynamic filters (now that data exists)
# =========================

with st.sidebar:
    # Date range
    cal = model["cal"]
    months = cal["Mes_fin"].tolist()
    min_m, max_m = months[0], months[-1]
    dr = st.slider(
        "Rango de fechas",
        min_value=min_m.to_pydatetime(),
        max_value=max_m.to_pydatetime(),
        value=(min_m.to_pydatetime(), max_m.to_pydatetime()),
        format="YYYY-MM",
    )
    dr_start = ensure_month_end(dr[0])
    dr_end = ensure_month_end(dr[1])

    # Client/project filters
    clients = sorted([c for c in fact_rev["Cliente"].dropna().unique().tolist() if str(c).strip() != ""])
    selected_clients = st.multiselect("Cliente(s)", options=clients, default=[])

    projects = fact_rev[["Proyecto_ID", "Proyecto_nombre"]].drop_duplicates()
    projects["label"] = projects["Proyecto_ID"].astype(str) + " — " + projects["Proyecto_nombre"].astype(str)
    project_labels = projects["label"].sort_values().tolist()
    selected_projects = st.multiselect("Proyecto(s)", options=project_labels, default=[])

    mode_board = st.toggle("Modo Directorio (resumen)", value=True)


def apply_filters_rev(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out = out[(out["Mes_facturacion"] >= dr_start) & (out["Mes_facturacion"] <= dr_end)]
    if selected_clients:
        out = out[out["Cliente"].isin(selected_clients)]
    if selected_projects:
        # map label back to IDs
        sel_ids = [s.split(" — ")[0] for s in selected_projects]
        out = out[out["Proyecto_ID"].isin(sel_ids)]
    return out


# Filtered views (for drill-down)
fact_rev_f = apply_filters_rev(fact_rev)

# Rebuild aggregates under filters (for coherence)
# (keep costs unfiltered by default; you can add cost center filters later)
pyg_f = pyg[(pyg["Mes_fin"] >= dr_start) & (pyg["Mes_fin"] <= dr_end)].copy()
cash_f = cash[(cash["Mes_fin"] >= dr_start) & (cash["Mes_fin"] <= dr_end)].copy()
iva_f = iva[(iva["Mes_fin"] >= dr_start) & (iva["Mes_fin"] <= dr_end)].copy()


# Currency conversion for displayed frames
money_cols_pyg = ["Ventas_netas", "Costos_directos", "Margen_bruto", "OPEX", "Personas", "EBITDA"]
money_cols_cash = ["Cobros", "NoOp_Neto", "Pagos_OPEX", "Pagos_CD", "Pagos_Personas", "Pago_IVA", "Flujo_neto", "Caja_ini_mes", "Caja_fin_mes"]
money_cols_iva = ["IVA_debito", "IVA_credito", "IVA_neto_mes", "IVA_por_pagar_mes", "IVA_pago_mes_siguiente", "IVA_remanente_fin_mes"]

pyg_disp = to_currency(pyg_f, money_cols_pyg, currency, fx)
cash_disp = to_currency(cash_f, money_cols_cash, currency, fx)
iva_disp = to_currency(iva_f, money_cols_iva, currency, fx)

kpis = build_kpis(pyg_f, cash_f, fact_rev_f)
kpis_disp = kpis.copy()
if currency == "USD":
    for k in ["Ventas_netas", "Margen_bruto", "EBITDA", "Caja_min", "Caja_fin", "Burn_prom"]:
        kpis_disp[k] = kpis_disp[k] / fx


# =========================
# Top KPI strip
# =========================

c1, c2, c3, c4, c5, c6 = st.columns(6)

c1.metric("Ventas netas", fmt_money(kpis_disp["Ventas_netas"]) + ("" if currency == "CLP" else " USD"))
c2.metric("Margen bruto", fmt_money(kpis_disp["Margen_bruto"]), fmt_pct(kpis["Margen_pct"]))
c3.metric("EBITDA", fmt_money(kpis_disp["EBITDA"]), fmt_pct(kpis["EBITDA_pct"]))
c4.metric("Caja mínima", fmt_money(kpis_disp["Caja_min"]))
c5.metric("Caja fin", fmt_money(kpis_disp["Caja_fin"]))
c6.metric("Conc. Top3 clientes", fmt_pct(kpis["Conc_top3"]))

if not np.isfinite(kpis["Runway_meses_aprox"]):
    runway_txt = "∞"
else:
    runway_txt = f"{kpis['Runway_meses_aprox']:.1f}"
st.caption(f"Runway aproximado (meses, usando burn promedio de meses negativos): **{runway_txt}**")


# =========================
# Tabs
# =========================

tab_resumen, tab_ingresos, tab_costos, tab_caja_iva, tab_escenarios, tab_calidad = st.tabs(
    ["Resumen", "Ingresos", "Costos", "Caja + IVA", "Escenarios", "Calidad de datos"]
)

# ---- Resumen ----
with tab_resumen:
    left, right = st.columns([1.2, 1.0], gap="large")

    with left:
        st.subheader("Caja acumulada")
        fig = px.line(
            cash_disp,
            x="Mes_fin",
            y="Caja_fin_mes",
            markers=True,
            title="Caja fin de mes",
        )
        fig.update_layout(height=380, xaxis_title="", yaxis_title=("USD" if currency == "USD" else "CLP"))
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Waterfall: Ventas → Margen → EBITDA")
        # One-year totals on filtered range
        total_ventas = float(pyg_disp["Ventas_netas"].sum())
        total_cd = float(pyg_disp["Costos_directos"].sum())
        total_opex = float(pyg_disp["OPEX"].sum())
        total_ppl = float(pyg_disp["Personas"].sum())

        wf = go.Figure(
            go.Waterfall(
                name="",
                orientation="v",
                measure=["relative", "relative", "relative", "relative", "total"],
                x=["Ventas netas", "- Costos directos", "- OPEX", "- Personas", "EBITDA"],
                y=[total_ventas, -total_cd, -total_opex, -total_ppl, 0],
            )
        )
        wf.update_layout(height=380, title="Descomposición EBITDA", yaxis_title=("USD" if currency == "USD" else "CLP"))
        st.plotly_chart(wf, use_container_width=True)

    with right:
        st.subheader("P&L mensual (neto)")
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(x=pyg_disp["Mes_fin"], y=pyg_disp["Ventas_netas"], name="Ventas netas"))
        fig2.add_trace(go.Bar(x=pyg_disp["Mes_fin"], y=pyg_disp["Costos_directos"], name="Costos directos"))
        fig2.add_trace(go.Bar(x=pyg_disp["Mes_fin"], y=pyg_disp["OPEX"], name="OPEX"))
        fig2.add_trace(go.Bar(x=pyg_disp["Mes_fin"], y=pyg_disp["Personas"], name="Personas"))
        fig2.update_layout(barmode="group", height=380, xaxis_title="", yaxis_title=("USD" if currency == "USD" else "CLP"))
        st.plotly_chart(fig2, use_container_width=True)

        if not mode_board:
            st.subheader("Tabla P&L (drill)")
            show = pyg_disp.copy()
            show["Margen_bruto_pct"] = show["Margen_bruto_pct"].apply(lambda x: x if pd.notna(x) else np.nan)
            show["EBITDA_pct"] = show["EBITDA_pct"].apply(lambda x: x if pd.notna(x) else np.nan)
            st.dataframe(show, use_container_width=True, hide_index=True)


# ---- Ingresos ----
with tab_ingresos:
    st.subheader("Devengo vs Cobro (bruto)")
    # rebuild cobranzas under filter
    cob_f = build_fact_cobranzas(fact_rev_f, model["cal"])
    cob_f = cob_f[(cob_f["Mes_fin"] >= dr_start) & (cob_f["Mes_fin"] <= dr_end)].copy()
    cob_disp = to_currency(cob_f, ["Cobros_brutos"], currency, fx)

    dev_bruto = fact_rev_f.groupby("Mes_facturacion", as_index=False)["Monto_bruto_hito"].sum().rename(
        columns={"Mes_facturacion": "Mes_fin", "Monto_bruto_hito": "Devengo_bruto"}
    )
    dev_bruto = dev_bruto[(dev_bruto["Mes_fin"] >= dr_start) & (dev_bruto["Mes_fin"] <= dr_end)].copy()
    dev_disp = to_currency(dev_bruto, ["Devengo_bruto"], currency, fx)

    t = pd.DataFrame({"Mes_fin": model["cal"]["Mes_fin"]})
    t = t.merge(dev_disp, on="Mes_fin", how="left").merge(cob_disp.rename(columns={"Cobros_brutos": "Cobros"}), on="Mes_fin", how="left")
    t = t[(t["Mes_fin"] >= dr_start) & (t["Mes_fin"] <= dr_end)].fillna(0.0)

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=t["Mes_fin"], y=t["Devengo_bruto"], mode="lines+markers", name="Devengo (bruto)"))
    fig.add_trace(go.Scatter(x=t["Mes_fin"], y=t["Cobros"], mode="lines+markers", name="Cobros (bruto)"))
    fig.update_layout(height=420, xaxis_title="", yaxis_title=("USD" if currency == "USD" else "CLP"))
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Mix: ventas netas por Cliente")
    by_cli = fact_rev_f.groupby("Cliente", as_index=False)["Monto_neto_hito"].sum().sort_values("Monto_neto_hito", ascending=False)
    by_cli_disp = to_currency(by_cli, ["Monto_neto_hito"], currency, fx)
    fig2 = px.bar(by_cli_disp.head(25), x="Cliente", y="Monto_neto_hito", title="Top clientes (neto)")
    fig2.update_layout(height=420, xaxis_title="", yaxis_title=("USD" if currency == "USD" else "CLP"))
    st.plotly_chart(fig2, use_container_width=True)

    if not mode_board:
        st.subheader("Detalle devengo (hitos)")
        detail = fact_rev_f.copy()
        if currency == "USD":
            for c in ["Monto_neto_hito", "IVA_hito", "Monto_bruto_hito"]:
                detail[c] = detail[c] / fx
        st.dataframe(detail.sort_values(["Mes_facturacion", "Cliente"]), use_container_width=True, hide_index=True)


# ---- Costos ----
with tab_costos:
    st.subheader("Estructura de costos (neto)")
    # aggregate monthly
    cd_m = fact_cd.groupby("Mes_fin", as_index=False)["CD_neto"].sum().rename(columns={"CD_neto": "Costos_directos"})
    opex_m = fact_opex.groupby("Mes_fin", as_index=False)["OPEX_neto"].sum().rename(columns={"OPEX_neto": "OPEX"})
    ppl_m = fact_ppl.groupby("Mes_fin", as_index=False)["Total_personas"].sum().rename(columns={"Total_personas": "Personas"})

    cost = pd.DataFrame({"Mes_fin": model["cal"]["Mes_fin"]})
    cost = cost.merge(cd_m, on="Mes_fin", how="left").merge(opex_m, on="Mes_fin", how="left").merge(ppl_m, on="Mes_fin", how="left")
    cost = cost[(cost["Mes_fin"] >= dr_start) & (cost["Mes_fin"] <= dr_end)].fillna(0.0)

    cost_disp = to_currency(cost, ["Costos_directos", "OPEX", "Personas"], currency, fx)

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=cost_disp["Mes_fin"], y=cost_disp["Costos_directos"], stackgroup="one", name="Costos directos"))
    fig.add_trace(go.Scatter(x=cost_disp["Mes_fin"], y=cost_disp["OPEX"], stackgroup="one", name="OPEX"))
    fig.add_trace(go.Scatter(x=cost_disp["Mes_fin"], y=cost_disp["Personas"], stackgroup="one", name="Personas"))
    fig.update_layout(height=420, xaxis_title="", yaxis_title=("USD" if currency == "USD" else "CLP"), title="Stack (neto)")
    st.plotly_chart(fig, use_container_width=True)

    colA, colB = st.columns(2, gap="large")
    with colA:
        st.subheader("OPEX por tipo")
        o = fact_opex.copy()
        o = o[(o["Mes_fin"] >= dr_start) & (o["Mes_fin"] <= dr_end)].copy()
        by_type = o.groupby("Tipo_gasto", as_index=False)["OPEX_neto"].sum().sort_values("OPEX_neto", ascending=False)
        by_type_disp = to_currency(by_type, ["OPEX_neto"], currency, fx)
        st.plotly_chart(px.pie(by_type_disp, values="OPEX_neto", names="Tipo_gasto", title="Distribución OPEX"), use_container_width=True)

    with colB:
        st.subheader("Costos directos por concepto")
        cd = fact_cd.copy()
        cd = cd[(cd["Mes_fin"] >= dr_start) & (cd["Mes_fin"] <= dr_end)].copy()
        by_cd = cd.groupby("Concepto", as_index=False)["CD_neto"].sum().sort_values("CD_neto", ascending=False)
        by_cd_disp = to_currency(by_cd, ["CD_neto"], currency, fx)
        st.plotly_chart(px.bar(by_cd_disp, x="Concepto", y="CD_neto", title="Costos directos (neto)"), use_container_width=True)

    if not mode_board:
        st.subheader("Detalle OPEX (drill)")
        o_detail = fact_opex[(fact_opex["Mes_fin"] >= dr_start) & (fact_opex["Mes_fin"] <= dr_end)].copy()
        if currency == "USD":
            for c in ["OPEX_neto", "IVA_credito_opex", "OPEX_bruto"]:
                o_detail[c] = o_detail[c] / fx
        st.dataframe(o_detail.sort_values(["Mes_fin", "Tipo_gasto", "Concepto"]), use_container_width=True, hide_index=True)


# ---- Caja + IVA ----
with tab_caja_iva:
    st.subheader("Flujo de caja mensual (componentes)")
    comp = cash_disp.copy()

    fig = go.Figure()
    fig.add_trace(go.Bar(x=comp["Mes_fin"], y=comp["Cobros"], name="Cobros"))
    fig.add_trace(go.Bar(x=comp["Mes_fin"], y=comp["NoOp_Neto"], name="No Operacionales"))
    fig.add_trace(go.Bar(x=comp["Mes_fin"], y=-comp["Pagos_OPEX"], name="- OPEX"))
    fig.add_trace(go.Bar(x=comp["Mes_fin"], y=-comp["Pagos_CD"], name="- Costos directos"))
    fig.add_trace(go.Bar(x=comp["Mes_fin"], y=-comp["Pagos_Personas"], name="- Personas"))
    fig.add_trace(go.Bar(x=comp["Mes_fin"], y=-comp["Pago_IVA"], name="- IVA pagado"))
    fig.update_layout(barmode="relative", height=420, xaxis_title="", yaxis_title=("USD" if currency == "USD" else "CLP"))
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("IVA (débito, crédito, neto y pago mes siguiente)")
    iva_show = iva_disp.copy()
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(x=iva_show["Mes_fin"], y=iva_show["IVA_debito"], mode="lines+markers", name="IVA débito"))
    fig2.add_trace(go.Scatter(x=iva_show["Mes_fin"], y=iva_show["IVA_credito"], mode="lines+markers", name="IVA crédito"))
    fig2.add_trace(go.Scatter(x=iva_show["Mes_fin"], y=iva_show["IVA_neto_mes"], mode="lines+markers", name="IVA neto mes"))
    fig2.add_trace(go.Scatter(x=iva_show["Mes_fin"], y=iva_show["IVA_pago_mes_siguiente"], mode="lines+markers", name="Pago IVA (mes siguiente)"))
    fig2.update_layout(height=420, xaxis_title="", yaxis_title=("USD" if currency == "USD" else "CLP"))
    st.plotly_chart(fig2, use_container_width=True)

    if not mode_board:
        st.subheader("Tabla caja (drill)")
        st.dataframe(cash_disp, use_container_width=True, hide_index=True)
        st.subheader("Tabla IVA (drill)")
        st.dataframe(iva_disp, use_container_width=True, hide_index=True)


# ---- Escenarios ----
with tab_escenarios:
    st.subheader("Comparador de escenarios (recalcula todo)")
    # compute for all scenarios
    scen_names = list(scenarios.keys())
    results = []
    cash_lines = []

    for s in scen_names:
        m = compute_model(excel_bytes, s)
        pyg_s = m["pyg"]
        cash_s = m["cash"]
        rev_s = m["fact_rev"]

        # apply date filter
        pyg_s = pyg_s[(pyg_s["Mes_fin"] >= dr_start) & (pyg_s["Mes_fin"] <= dr_end)].copy()
        cash_s = cash_s[(cash_s["Mes_fin"] >= dr_start) & (cash_s["Mes_fin"] <= dr_end)].copy()
        rev_s = rev_s[(rev_s["Mes_facturacion"] >= dr_start) & (rev_s["Mes_facturacion"] <= dr_end)].copy()

        k = build_kpis(pyg_s, cash_s, rev_s)
        k["Escenario"] = s
        results.append(k)

        line = cash_s[["Mes_fin", "Caja_fin_mes"]].copy()
        line["Escenario"] = s
        cash_lines.append(line)

    kdf = pd.DataFrame(results)
    # convert to display
    if currency == "USD":
        for c in ["Ventas_netas", "Margen_bruto", "EBITDA", "Caja_min", "Caja_fin", "Burn_prom"]:
            kdf[c] = kdf[c] / fx

    st.dataframe(
        kdf[["Escenario", "Ventas_netas", "Margen_bruto", "EBITDA", "Caja_min", "Caja_fin", "Conc_top3", "Runway_meses_aprox"]],
        use_container_width=True,
        hide_index=True
    )

    lines = pd.concat(cash_lines, ignore_index=True)
    if currency == "USD":
        lines["Caja_fin_mes"] = lines["Caja_fin_mes"] / fx
    fig = px.line(lines, x="Mes_fin", y="Caja_fin_mes", color="Escenario", markers=True, title="Caja fin de mes por escenario")
    fig.update_layout(height=450, xaxis_title="", yaxis_title=("USD" if currency == "USD" else "CLP"))
    st.plotly_chart(fig, use_container_width=True)

    st.caption("Tip: este comparador es el que más sirve para directorio (decisión por runway/caja mínima).")


# ---- Calidad ----
with tab_calidad:
    st.subheader("Checks y consistencia")
    checks = run_checks(model)

    if len(checks) == 0:
        st.success("Sin observaciones críticas detectadas con los checks actuales.")
    else:
        st.warning("Se detectaron observaciones. Revísalas: afectan coherencia de ingresos/hitos.")
        st.dataframe(checks, use_container_width=True, hide_index=True)

    st.subheader("Supuestos de cálculo (para transparencia)")
    st.markdown(
        """
- **DSO → meses**: se convierte como `ceil(DSO_días/30)` y se desplaza la cobranza a `Mes_facturacion + shift_meses`.
- **IVA**: IVA débito (ventas) ocurre en el mes de facturación; IVA crédito (compras) en el mes del gasto.
- **Pago IVA**: se modela como **pago al mes siguiente** del IVA “por pagar” del mes anterior (aprox. declaración mensual).
- **Remanente IVA**: si el neto es negativo, se arrastra como crédito (no se modela devolución automática; si hay devolución está en No Operacionales).
- **Costos directos**: si vienen negativos en la matriz mensual, se normalizan a magnitud positiva para cálculos.
        """
    )

    if not mode_board:
        st.subheader("Data room (inputs cargados)")
        st.write("**04_PROYECTOS (preview)**")
        st.dataframe(model["fact_rev"][["Proyecto_ID", "Cliente", "Tipo_cliente", "Estado"]].drop_duplicates().head(20), use_container_width=True, hide_index=True)


# Footer
st.divider()
st.caption("Modelo recalculado desde inputs del Excel (sin depender de fórmulas cacheadas).")
