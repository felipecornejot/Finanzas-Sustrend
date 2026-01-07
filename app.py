# app.py
# Sustrend — Modelo Financiero 2026 (Streamlit)
# Dashboard dinámico recalculado desde inputs del Excel
# + Cross-filter por click (Plotly events)
# + Default Excel desde GitHub (raw) + upload opcional
# + Parsing robusto de escenarios/probabilidades

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Any

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st

# Click events (Plotly -> Streamlit)
try:
    from streamlit_plotly_events import plotly_events
    HAS_PLOTLY_EVENTS = True
except Exception:
    HAS_PLOTLY_EVENTS = False


# =========================================================
# CONFIG
# =========================================================

# 👉 Reemplaza por tu RAW URL real en GitHub
DEFAULT_XLSX_URL = "https://raw.githubusercontent.com/felipecornejot/Finanzas-Sustrend/main/Sustrend_Modelo_Financiero_2026_v4_dashboard_graficos.xlsx
"

DEFAULT_EXCHANGE_RATE = 950.0  # CLP por 1 USD (ajustable en sidebar)

st.set_page_config(
    page_title="Sustrend — Modelo Financiero 2026",
    page_icon="📊",
    layout="wide",
)

CSS = """
<style>
.block-container { padding-top: 1.2rem; padding-bottom: 2rem; }
header[data-testid="stHeader"] { background: rgba(0,0,0,0); }
div[data-testid="stMetric"] { padding: 0.2rem 0.6rem; border-radius: 0.75rem; }
div[data-testid="stDataFrame"] * { font-size: 0.92rem; }
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)


# =========================================================
# SESSION STATE
# =========================================================

def _init_state():
    ss = st.session_state
    ss.setdefault("clients_ms", [])
    ss.setdefault("projects_ms", [])
    ss.setdefault("date_range_slider", None)  # set later once calendar known
    ss.setdefault("mode_board", True)
    ss.setdefault("click_filters_on", True)

    # Click-driven state
    ss.setdefault("clicked_client", None)
    ss.setdefault("clicked_project_label", None)
    ss.setdefault("clicked_month", None)  # pd.Timestamp month-end

_init_state()


def clear_click_filters():
    st.session_state.clicked_client = None
    st.session_state.clicked_project_label = None
    st.session_state.clicked_month = None


# =========================================================
# TYPES
# =========================================================

@dataclass(frozen=True)
class Scenario:
    name: str
    mult_precio: float
    mult_personas: float
    mult_opex_fijo: float
    mult_opex_variable: float
    mult_costo_directo: float
    dso_privado: int
    dso_publico_lt10m: int
    dso_publico_gte10m: int
    prob_by_estado: Dict[str, float]


@dataclass(frozen=True)
class Params:
    anio: int
    tasa_iva: float
    saldo_inicial_caja: float
    umbral_publico_10m: float
    iva_remanente_inicial: float


# =========================================================
# UTILS
# =========================================================

def ensure_month_end(x) -> pd.Timestamp:
    ts = pd.to_datetime(x, errors="coerce")
    if pd.isna(ts):
        return ts
    return (ts + pd.offsets.MonthEnd(0)).normalize()


def month_end_add(month_end: pd.Timestamp, months: int) -> pd.Timestamp:
    if pd.isna(month_end):
        return month_end
    return (month_end + pd.offsets.MonthEnd(months)).normalize()


def ceil_div_days_to_months(dso_days: float) -> int:
    if pd.isna(dso_days):
        return 0
    d = float(dso_days)
    if d <= 0:
        return 0
    return int(math.ceil(d / 30.0))


def _norm_text(x: Any) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return ""
    return str(x).strip()


def to_float_safe(x: Any, default: float = np.nan) -> float:
    """
    Convierte números que pueden venir como:
    - 0.3
    - "0,3"
    - "30%"
    - " 0,30 "
    - "-"
    - None/NaN
    - "1.234,56" (formato CL)
    """
    if x is None:
        return default
    if isinstance(x, (int, float, np.number)):
        if pd.isna(x):
            return default
        return float(x)

    s = str(x).strip()
    if s == "" or s in {"-", "—", "–", "NA", "N/A", "nan", "NaN"}:
        return default

    # porcentaje
    is_pct = False
    if "%" in s:
        is_pct = True
        s = s.replace("%", "").strip()

    # normaliza separadores (manejo CL: miles "." y decimal ",")
    # casos:
    # - "1.234,56" => "1234.56"
    # - "1234,56"  => "1234.56"
    # - "1,234.56" => probablemente US, pero lo manejamos: si hay ambos, asumimos decimal es el último
    if "," in s and "." in s:
        # decide por el último separador como decimal
        if s.rfind(",") > s.rfind("."):
            # CL-like
            s = s.replace(".", "")
            s = s.replace(",", ".")
        else:
            # US-like
            s = s.replace(",", "")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")
    # si solo "." está ok

    try:
        v = float(s)
        if is_pct:
            v = v / 100.0
        return v
    except Exception:
        return default


def melt_month_matrix(df: pd.DataFrame, id_cols: List[str], value_name: str = "Monto") -> pd.DataFrame:
    month_cols = []
    for c in df.columns:
        if c == "Total_Año":
            continue
        if isinstance(c, (pd.Timestamp, np.datetime64)):
            month_cols.append(c)
        else:
            try:
                dt = pd.to_datetime(c, errors="raise")
                if dt is not None:
                    month_cols.append(c)
            except Exception:
                pass

    keep_cols = [c for c in id_cols if c in df.columns] + month_cols
    tmp = df[keep_cols].copy()

    melted = tmp.melt(id_vars=[c for c in id_cols if c in tmp.columns], var_name="Mes_fin_raw", value_name=value_name)
    melted["Mes_fin"] = melted["Mes_fin_raw"].apply(ensure_month_end)
    melted.drop(columns=["Mes_fin_raw"], inplace=True)
    return melted


# =========================================================
# FILE SOURCE (GitHub default + upload)
# =========================================================

@st.cache_data(show_spinner=False)
def fetch_default_excel_bytes(url: str) -> bytes:
    if not url or "raw.githubusercontent.com" not in url:
        raise ValueError("DEFAULT_XLSX_URL no es una raw URL válida de GitHub.")
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.content


@st.cache_data(show_spinner=False)
def read_sheet(excel_bytes: bytes, sheet_name: str, header: Optional[int] = None) -> pd.DataFrame:
    return pd.read_excel(excel_bytes, sheet_name=sheet_name, header=header, engine="openpyxl")


# =========================================================
# PARSERS (Params + Scenarios robusto)
# =========================================================

def parse_params(excel_bytes: bytes) -> Params:
    raw = read_sheet(excel_bytes, "02_PARAMETROS", header=None)
    tbl = raw.iloc[2:].copy()
    tbl.columns = ["Nombre", "Valor", "Descripcion"]
    tbl = tbl.dropna(subset=["Nombre"]).copy()
    tbl["Nombre"] = tbl["Nombre"].astype(str).str.strip()

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


def _find_row_index(raw: pd.DataFrame, needle: str) -> Optional[int]:
    """
    Busca needle en la primera columna (robusto: acentos/espacios/case)
    """
    col0 = raw.iloc[:, 0].astype(str).fillna("").map(lambda s: s.strip().lower())
    n = needle.strip().lower()
    # tolera "parametro" vs "parámetro"
    n2 = n.replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")
    for i, v in enumerate(col0.tolist()):
        vv = v.replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")
        if vv == n2:
            return raw.index[i]
    return None


def parse_scenarios(excel_bytes: bytes) -> Dict[str, Scenario]:
    raw = read_sheet(excel_bytes, "02B_ESCENARIOS", header=None)

    a0 = _find_row_index(raw, "Parámetro") or _find_row_index(raw, "Parametro")
    if a0 is None:
        raise ValueError("No se encontró la tabla de parámetros (fila 'Parámetro') en 02B_ESCENARIOS.")

    b0 = _find_row_index(raw, "Estado")
    if b0 is None:
        raise ValueError("No se encontró la tabla de probabilidades (fila 'Estado') en 02B_ESCENARIOS.")

    # ---- Tabla A: parámetros ----
    a = raw.loc[a0:].copy()
    a.columns = ["Parametro", "Base", "Conservador", "Agresivo"] + list(a.columns[4:])
    a = a[["Parametro", "Base", "Conservador", "Agresivo"]]
    a = a.dropna(subset=["Parametro"]).copy()
    a["Parametro"] = a["Parametro"].astype(str).str.strip()

    # recorta antes de que empiece la tabla Estado (si aparece más abajo)
    if b0 > a0:
        a = a.loc[: b0 - 1].copy()

    # ---- Tabla B: probabilidades por estado ----
    b = raw.loc[b0:].copy()
    b.columns = ["Estado", "Base", "Conservador", "Agresivo"] + list(b.columns[4:])
    b = b[["Estado", "Base", "Conservador", "Agresivo"]].dropna(subset=["Estado"]).copy()
    b["Estado"] = b["Estado"].astype(str).str.strip()

    def get_param(df: pd.DataFrame, name: str, scen: str, default=None):
        s = df.loc[df["Parametro"] == name, scen]
        if len(s) == 0:
            return default
        v = s.iloc[0]
        if pd.isna(v):
            return default
        # admite coma/% etc
        vv = to_float_safe(v, default=np.nan)
        if np.isnan(vv):
            return default
        return vv

    def prob_map(df: pd.DataFrame, scen: str) -> Dict[str, float]:
        out: Dict[str, float] = {}
        for _, r in df.iterrows():
            estado = str(r["Estado"]).strip()
            if not estado:
                continue
            v = to_float_safe(r.get(scen), default=np.nan)

            # Si viene vacío o no parseable: asumimos 0 (no 1) para no inflar
            if np.isnan(v):
                v = 0.0
            # clamp
            v = max(0.0, min(1.0, float(v)))
            out[estado] = v

        # Normaliza si no suma ~1 (y si suma > 0)
        s = sum(out.values())
        if s > 0 and not (0.98 <= s <= 1.02):
            out = {k: v / s for k, v in out.items()}
        # Si todo era 0, fallback seguro: todo 1 para evitar dividir por cero,
        # pero solo si hay estados. (equivale a “no aplico prob.”)
        if (sum(out.values()) == 0) and len(out) > 0:
            out = {k: 1.0 for k in out.keys()}
        return out

    scenarios: Dict[str, Scenario] = {}
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


# =========================================================
# LOAD INPUTS
# =========================================================

@st.cache_data(show_spinner=False)
def load_inputs(excel_bytes: bytes) -> Dict[str, pd.DataFrame]:
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


# =========================================================
# CLEANERS
# =========================================================

def clean_calendar(df_cal: pd.DataFrame) -> pd.DataFrame:
    out = df_cal.copy()
    out["Mes_fin"] = out["Mes_fin"].apply(ensure_month_end)
    out["Mes_idx"] = pd.to_numeric(out["Mes_idx"], errors="coerce").fillna(0).astype(int)
    return out.sort_values("Mes_fin")


def clean_projects(df_proj: pd.DataFrame, params: Params) -> pd.DataFrame:
    out = df_proj.copy()
    for c in ["Mes_inicio", "Mes_fin"]:
        if c in out.columns:
            out[c] = out[c].apply(ensure_month_end)
    out["IVA_tasa"] = pd.to_numeric(out.get("IVA_tasa", params.tasa_iva), errors="coerce").fillna(params.tasa_iva).astype(float)
    out["Tipo_cliente"] = out.get("Tipo_cliente", "").astype(str).str.strip()
    out["Estado"] = out.get("Estado", "").astype(str).str.strip()
    out["Monto_neto_CLP"] = pd.to_numeric(out.get("Monto_neto_CLP", 0.0), errors="coerce").fillna(0.0)
    out["Cliente"] = out.get("Cliente", "").astype(str).str.strip()
    return out


def clean_hitos(df_hitos: pd.DataFrame) -> pd.DataFrame:
    out = df_hitos.copy()
    out["Mes_facturacion"] = out["Mes_facturacion"].apply(ensure_month_end)
    out["Pct_facturacion"] = pd.to_numeric(out["Pct_facturacion"], errors="coerce").fillna(0.0)
    out["Glosa"] = out.get("Glosa", "").astype(str)
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
    if "CD_ID" in mens.columns:
        mens = mens[mens["CD_ID"].notna()].copy()
        mens = mens[mens["CD_ID"].astype(str).str.upper() != "TOTAL_CD"].copy()
    return meta, mens


def clean_noop(df_noop: pd.DataFrame) -> pd.DataFrame:
    out = df_noop.copy()
    out["Tipo"] = out["Tipo"].astype(str).str.strip()
    return out


# =========================================================
# FACT BUILDERS
# =========================================================

def resolve_project_dso_days(row_proj: pd.Series, scenario: Scenario) -> int:
    if "DSO_dias_override" in row_proj and not pd.isna(row_proj["DSO_dias_override"]):
        try:
            return int(row_proj["DSO_dias_override"])
        except Exception:
            pass

    tipo = str(row_proj.get("Tipo_cliente", "")).strip()
    if tipo == "Privado":
        return scenario.dso_privado
    if tipo == "Publico_<10M":
        return scenario.dso_publico_lt10m
    if tipo == "Publico_>=10M":
        return scenario.dso_publico_gte10m
    return scenario.dso_privado


def prob_from_estado(estado: str, scenario: Scenario) -> float:
    e = str(estado).strip()
    return float(scenario.prob_by_estado.get(e, 1.0))


def build_fact_revenue_devengo(
    cal: pd.DataFrame,
    proyectos: pd.DataFrame,
    hitos: pd.DataFrame,
    scenario: Scenario,
) -> pd.DataFrame:
    p = proyectos.copy()
    h = hitos.copy()

    base = h.merge(p, on="Proyecto_ID", how="left", suffixes=("", "_proj"))
    base["Prob_conv"] = base["Estado"].apply(lambda x: prob_from_estado(x, scenario))

    base["Monto_neto_hito"] = (
        base["Monto_neto_CLP"]
        * base["Pct_facturacion"]
        * scenario.mult_precio
        * base["Prob_conv"]
    )
    base["IVA_hito"] = base["Monto_neto_hito"] * base["IVA_tasa"]
    base["Monto_bruto_hito"] = base["Monto_neto_hito"] + base["IVA_hito"]

    months = set(cal["Mes_fin"].tolist())
    base = base[base["Mes_facturacion"].isin(months)].copy()

    base["DSO_dias_eff"] = base.apply(lambda r: resolve_project_dso_days(r, scenario), axis=1)

    if "DSO_dias" in base.columns:
        base["DSO_dias_eff"] = np.where(
            base["DSO_dias"].notna(),
            base["DSO_dias"].astype(float),
            base["DSO_dias_eff"].astype(float),
        )

    base["Shift_meses"] = base["DSO_dias_eff"].apply(ceil_div_days_to_months).astype(int)
    base["Mes_cobro"] = base.apply(lambda r: month_end_add(r["Mes_facturacion"], int(r["Shift_meses"])), axis=1)

    keep = [
        "Proyecto_ID", "Proyecto_nombre", "Cliente", "Tipo_cliente", "Estado",
        "Mes_facturacion", "Hito_n", "Glosa", "Pct_facturacion",
        "Monto_neto_hito", "IVA_hito", "Monto_bruto_hito",
        "DSO_dias_eff", "Shift_meses", "Mes_cobro",
    ]
    return base[keep].copy()


def build_fact_cobranzas(fact_rev: pd.DataFrame, cal: pd.DataFrame) -> pd.DataFrame:
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
    months = cal["Mes_fin"].tolist()
    month_set = set(months)
    base_rows = []

    for _, r in opex.iterrows():
        rid = r["OPEX_ID"]
        rec = str(r["Recurrencia"])
        tipo_gasto = str(r["Tipo_gasto"])
        centro = str(r.get("Centro_costo", "N/A"))
        iva_af = str(r["IVA_afecto"]).upper() == "SI"
        monto = float(r["Monto_neto"])

        mult = scenario.mult_opex_fijo if tipo_gasto.lower() == "fijo" else scenario.mult_opex_variable
        monto_eff = monto * mult

        if rec == "Mensual":
            start = r["Mes_inicio"]
            end = r["Mes_fin"]
            if pd.isna(start) or pd.isna(end):
                continue
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
            pass

    base = pd.DataFrame(
        base_rows,
        columns=["OPEX_ID", "Concepto", "Tipo_gasto", "Centro_costo", "IVA_afecto_bool", "Mes_fin", "OPEX_neto"],
    )

    custom = opex_custom.copy()
    if len(custom) > 0:
        id_cols = ["OPEX_ID", "Concepto"]
        m = melt_month_matrix(custom, id_cols=id_cols, value_name="OPEX_neto_raw")
        m["OPEX_neto_raw"] = pd.to_numeric(m["OPEX_neto_raw"], errors="coerce").fillna(0.0)

        meta = opex[["OPEX_ID", "Tipo_gasto", "Centro_costo", "IVA_afecto"]].copy()
        meta["IVA_afecto_bool"] = meta["IVA_afecto"].astype(str).str.upper().eq("SI")

        m = m.merge(meta, on="OPEX_ID", how="left")
        m["Tipo_gasto"] = m["Tipo_gasto"].fillna("Fijo")
        m["Centro_costo"] = m["Centro_costo"].fillna("N/A")

        m["mult"] = np.where(m["Tipo_gasto"].str.lower() == "fijo", scenario.mult_opex_fijo, scenario.mult_opex_variable)
        m["OPEX_neto"] = m["OPEX_neto_raw"] * m["mult"]
        m = m[["OPEX_ID", "Concepto", "Tipo_gasto", "Centro_costo", "IVA_afecto_bool", "Mes_fin", "OPEX_neto"]].copy()

        base = pd.concat([base, m], ignore_index=True)

    base = base[base["Mes_fin"].isin(month_set)].copy()
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
    id_cols = ["CD_ID", "Concepto"]
    m = melt_month_matrix(cd_mensual, id_cols=id_cols, value_name="CD_neto_raw")
    m["CD_neto_raw"] = pd.to_numeric(m["CD_neto_raw"], errors="coerce").fillna(0.0)
    m["CD_neto_raw"] = m["CD_neto_raw"].abs()

    meta = cd_meta[["CD_ID", "IVA_afecto", "Centro_costo"]].copy()
    meta["IVA_afecto_bool"] = meta["IVA_afecto"].astype(str).str.upper().eq("SI")

    m = m.merge(meta, on="CD_ID", how="left")
    m["IVA_afecto_bool"] = m["IVA_afecto_bool"].fillna(True)
    m["Centro_costo"] = m["Centro_costo"].fillna("N/A")

    m["CD_neto"] = m["CD_neto_raw"] * scenario.mult_costo_directo
    m["IVA_credito_cd"] = np.where(m["IVA_afecto_bool"], m["CD_neto"] * params.tasa_iva, 0.0)
    m["CD_bruto"] = m["CD_neto"] + m["IVA_credito_cd"]

    month_set = set(cal["Mes_fin"].tolist())
    m = m[m["Mes_fin"].isin(month_set)].copy()
    return m[["CD_ID", "Concepto", "Centro_costo", "Mes_fin", "CD_neto", "IVA_credito_cd", "CD_bruto"]].copy()


def build_fact_personas_monthly(
    cal: pd.DataFrame,
    personas: pd.DataFrame,
    scenario: Scenario,
) -> pd.DataFrame:
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

    return pd.DataFrame(
        rows,
        columns=["Persona_ID", "Nombre", "Rol", "Centro_costo", "Mes_fin", "Pago", "Previred", "Total_personas"],
    )


def build_fact_no_operacionales_monthly(
    cal: pd.DataFrame,
    noop_mensual: pd.DataFrame,
) -> pd.DataFrame:
    id_cols = ["NOP_ID", "Concepto", "Tipo"]
    m = melt_month_matrix(noop_mensual, id_cols=id_cols, value_name="NOP_raw")
    m["NOP_raw"] = pd.to_numeric(m["NOP_raw"], errors="coerce").fillna(0.0)
    m["sign"] = np.where(m["Tipo"].astype(str).str.lower().str.contains("ingreso"), 1.0, -1.0)
    m["NOP_monto"] = m["NOP_raw"] * m["sign"]

    month_set = set(cal["Mes_fin"].tolist())
    m = m[m["Mes_fin"].isin(month_set)].copy()
    return m[["NOP_ID", "Concepto", "Tipo", "Mes_fin", "NOP_monto"]].copy()


# =========================================================
# IVA, P&L, CASH, KPIs
# =========================================================

def build_iva_ledger(
    cal: pd.DataFrame,
    fact_rev: pd.DataFrame,
    fact_opex: pd.DataFrame,
    fact_cd: pd.DataFrame,
    params: Params,
) -> pd.DataFrame:
    months = cal["Mes_fin"].tolist()

    iva_deb = (
        fact_rev.groupby("Mes_facturacion", as_index=False)["IVA_hito"].sum()
        .rename(columns={"Mes_facturacion": "Mes_fin", "IVA_hito": "IVA_debito"})
    )

    iva_cred_opex = fact_opex.groupby("Mes_fin", as_index=False)["IVA_credito_opex"].sum()
    iva_cred_cd = fact_cd.groupby("Mes_fin", as_index=False)["IVA_credito_cd"].sum()
    iva_cred = iva_cred_opex.merge(iva_cred_cd, on="Mes_fin", how="outer").fillna(0.0)
    iva_cred["IVA_credito"] = iva_cred["IVA_credito_opex"] + iva_cred["IVA_credito_cd"]
    iva_cred = iva_cred[["Mes_fin", "IVA_credito"]]

    iva = pd.DataFrame({"Mes_fin": months})
    iva = iva.merge(iva_deb, on="Mes_fin", how="left").merge(iva_cred, on="Mes_fin", how="left")
    iva[["IVA_debito", "IVA_credito"]] = iva[["IVA_debito", "IVA_credito"]].fillna(0.0)

    iva["IVA_neto_mes"] = iva["IVA_debito"] - iva["IVA_credito"]

    rem = params.iva_remanente_inicial
    payable_by_month = []
    rem_by_month = []
    for _, r in iva.iterrows():
        net = float(r["IVA_neto_mes"])
        saldo = net + rem
        pay = max(saldo, 0.0)
        rem = min(saldo, 0.0)
        payable_by_month.append(pay)
        rem_by_month.append(rem)

    iva["IVA_por_pagar_mes"] = payable_by_month
    iva["IVA_remanente_fin_mes"] = rem_by_month
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

    cf = cf.merge(cobranzas.rename(columns={"Cobros_brutos": "Cobros"}), on="Mes_fin", how="left")
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
        cf["Cobros"] + cf["NoOp_Neto"]
        - cf["Pagos_OPEX"] - cf["Pagos_CD"] - cf["Pagos_Personas"] - cf["Pago_IVA"]
    )

    saldo = params.saldo_inicial_caja
    saldos = []
    for _, r in cf.iterrows():
        saldo += float(r["Flujo_neto"])
        saldos.append(saldo)

    cf["Caja_fin_mes"] = saldos
    cf["Caja_ini_mes"] = [params.saldo_inicial_caja] + saldos[:-1]
    return cf


def build_kpis(pyg: pd.DataFrame, cash: pd.DataFrame, fact_rev: pd.DataFrame) -> Dict[str, float]:
    ventas = float(pyg["Ventas_netas"].sum())
    margen = float(pyg["Margen_bruto"].sum())
    ebitda = float(pyg["EBITDA"].sum())
    caja_min = float(cash["Caja_fin_mes"].min())
    caja_fin = float(cash["Caja_fin_mes"].iloc[-1])

    neg = cash.loc[cash["Flujo_neto"] < 0, "Flujo_neto"]
    burn_prom = float(-neg.mean()) if len(neg) else 0.0
    runway = float("inf") if burn_prom <= 0 else max(0.0, cash_fin := cash["Caja_fin_mes"].iloc[-1]) / burn_prom

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


# =========================================================
# MODEL PIPELINE
# =========================================================

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


# =========================================================
# FORMATTING
# =========================================================

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
    out = df.copy()
    if mode == "USD":
        for c in cols:
            if c in out.columns:
                out[c] = out[c] / fx
    return out


# =========================================================
# UI
# =========================================================

st.title("📊 Sustrend — Modelo Financiero 2026 (dinámico)")

with st.sidebar:
    st.subheader("Fuente de datos (Excel)")
    source = st.radio(
        "Selecciona fuente",
        options=["Usar Excel por defecto (GitHub)", "Subir Excel (override)"],
        index=0,
    )

    uploaded = None
    excel_bytes: Optional[bytes] = None

    if source == "Subir Excel (override)":
        uploaded = st.file_uploader("Sube el Excel (.xlsx)", type=["xlsx"])
        if uploaded is None:
            st.info("Sube un archivo para continuar (o cambia a GitHub).")
            st.stop()
        excel_bytes = uploaded.getvalue()
    else:
        # GitHub default
        try:
            with st.spinner("Descargando Excel por defecto desde GitHub..."):
                excel_bytes = fetch_default_excel_bytes(DEFAULT_XLSX_URL)
            st.caption("✅ Excel cargado desde GitHub (raw).")
        except Exception as e:
            st.error(f"No pude descargar el Excel por defecto. Revisa DEFAULT_XLSX_URL.\n\nDetalle: {e}")
            st.stop()

    st.divider()
    st.subheader("Escenario")
    try:
        scenarios = parse_scenarios(excel_bytes)
        scen = st.selectbox("Escenario", options=list(scenarios.keys()), index=0)
    except Exception as e:
        st.error(f"Error leyendo escenarios (02B_ESCENARIOS): {e}")
        st.stop()

    st.subheader("Conversión (opcional)")
    currency = st.radio("Moneda", ["CLP", "USD"], horizontal=True)
    fx = st.number_input("Tipo de cambio (CLP por 1 USD)", min_value=1.0, value=float(DEFAULT_EXCHANGE_RATE), step=1.0)

    st.subheader("Click-to-filter")
    if not HAS_PLOTLY_EVENTS:
        st.warning("Falta `streamlit-plotly-events`. Instala y reinicia para habilitar click-to-filter.")
        st.session_state.click_filters_on = False
    st.session_state.click_filters_on = st.toggle("Activar filtros por click", value=st.session_state.click_filters_on)

    ccol1, ccol2 = st.columns(2)
    with ccol1:
        if st.button("Limpiar filtros click", use_container_width=True):
            clear_click_filters()
            st.rerun()
    with ccol2:
        if st.button("Limpiar todo", use_container_width=True):
            clear_click_filters()
            st.session_state.clients_ms = []
            st.session_state.projects_ms = []
            st.session_state.date_range_slider = None
            st.rerun()

    st.subheader("Filtros")


# =========================
# Compute model
# =========================

with st.spinner("Calculando modelo desde inputs..."):
    model = compute_model(excel_bytes, scen)

params_row = model["params"].iloc[0].to_dict()
params = Params(**params_row)

cal = model["cal"]
months = cal["Mes_fin"].tolist()
min_m, max_m = months[0], months[-1]

# Initialize date_range_slider once
if st.session_state.date_range_slider is None:
    st.session_state.date_range_slider = (min_m.to_pydatetime(), max_m.to_pydatetime())

# Build selectors universe
fact_rev = model["fact_rev"]
projects = fact_rev[["Proyecto_ID", "Proyecto_nombre"]].drop_duplicates()
projects["label"] = projects["Proyecto_ID"].astype(str) + " — " + projects["Proyecto_nombre"].astype(str)
project_labels = projects["label"].sort_values().tolist()

clients = sorted([c for c in fact_rev["Cliente"].dropna().unique().tolist() if str(c).strip() != ""])

# Apply click-driven filters into widgets
if st.session_state.click_filters_on:
    if st.session_state.clicked_client is not None:
        st.session_state.clients_ms = [st.session_state.clicked_client]
    if st.session_state.clicked_project_label is not None:
        st.session_state.projects_ms = [st.session_state.clicked_project_label]
    if st.session_state.clicked_month is not None:
        m = st.session_state.clicked_month
        st.session_state.date_range_slider = (m.to_pydatetime(), m.to_pydatetime())

with st.sidebar:
    dr = st.slider(
        "Rango de fechas",
        min_value=min_m.to_pydatetime(),
        max_value=max_m.to_pydatetime(),
        value=st.session_state.date_range_slider,
        key="date_range_slider",
        format="YYYY-MM",
    )
    dr_start = ensure_month_end(dr[0])
    dr_end = ensure_month_end(dr[1])

    selected_clients = st.multiselect("Cliente(s)", options=clients, key="clients_ms", default=st.session_state.clients_ms)
    selected_projects = st.multiselect("Proyecto(s)", options=project_labels, key="projects_ms", default=st.session_state.projects_ms)

    st.session_state.mode_board = st.toggle("Modo Directorio (resumen)", value=st.session_state.mode_board)
    mode_board = st.session_state.mode_board


def apply_filters_rev(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out = out[(out["Mes_facturacion"] >= dr_start) & (out["Mes_facturacion"] <= dr_end)]
    if selected_clients:
        out = out[out["Cliente"].isin(selected_clients)]
    if selected_projects:
        sel_ids = [s.split(" — ")[0] for s in selected_projects]
        out = out[out["Proyecto_ID"].isin(sel_ids)]
    return out


# Filtered views
fact_rev_f = apply_filters_rev(fact_rev)
pyg = model["pyg"]
cash = model["cash"]
iva = model["iva"]
fact_opex = model["fact_opex"]
fact_cd = model["fact_cd"]
fact_ppl = model["fact_ppl"]
fact_noop = model["fact_noop"]

pyg_f = pyg[(pyg["Mes_fin"] >= dr_start) & (pyg["Mes_fin"] <= dr_end)].copy()
cash_f = cash[(cash["Mes_fin"] >= dr_start) & (cash["Mes_fin"] <= dr_end)].copy()
iva_f = iva[(iva["Mes_fin"] >= dr_start) & (iva["Mes_fin"] <= dr_end)].copy()

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
# KPI strip
# =========================

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Ventas netas", fmt_money(kpis_disp["Ventas_netas"]) + ("" if currency == "CLP" else " USD"))
c2.metric("Margen bruto", fmt_money(kpis_disp["Margen_bruto"]), fmt_pct(kpis["Margen_pct"]))
c3.metric("EBITDA", fmt_money(kpis_disp["EBITDA"]), fmt_pct(kpis["EBITDA_pct"]))
c4.metric("Caja mínima", fmt_money(kpis_disp["Caja_min"]))
c5.metric("Caja fin", fmt_money(kpis_disp["Caja_fin"]))
c6.metric("Conc. Top3 clientes", fmt_pct(kpis["Conc_top3"]))

runway_txt = "∞" if not np.isfinite(kpis["Runway_meses_aprox"]) else f"{kpis['Runway_meses_aprox']:.1f}"
st.caption(f"Runway aproximado (meses, usando burn promedio de meses negativos): **{runway_txt}**")


# =========================
# Tabs
# =========================

tab_resumen, tab_ingresos, tab_costos, tab_caja_iva, tab_escenarios = st.tabs(
    ["Resumen", "Ingresos", "Costos", "Caja + IVA", "Escenarios"]
)

# ---- Resumen ----
with tab_resumen:
    left, right = st.columns([1.2, 1.0], gap="large")

    with left:
        st.subheader("Caja acumulada (click en un mes para filtrar)")
        fig = px.line(cash_disp, x="Mes_fin", y="Caja_fin_mes", markers=True, title="Caja fin de mes")
        fig.update_layout(height=380, xaxis_title="", yaxis_title=("USD" if currency == "USD" else "CLP"))

        if HAS_PLOTLY_EVENTS and st.session_state.click_filters_on:
            ev = plotly_events(fig, click_event=True, hover_event=False, select_event=False, key="evt_cash_month")
            if ev:
                x = ev[0].get("x")
                m = ensure_month_end(x)
                st.session_state.clicked_month = m
                st.rerun()
        else:
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Waterfall: Ventas → Margen → EBITDA")
        total_ventas = float(pyg_disp["Ventas_netas"].sum())
        total_cd = float(pyg_disp["Costos_directos"].sum())
        total_opex = float(pyg_disp["OPEX"].sum())
        total_ppl = float(pyg_disp["Personas"].sum())

        wf = go.Figure(
            go.Waterfall(
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
            st.dataframe(pyg_disp, use_container_width=True, hide_index=True)


# ---- Ingresos ----
with tab_ingresos:
    st.subheader("Devengo vs Cobro (bruto)")
    cob_f = build_fact_cobranzas(fact_rev_f, cal)
    cob_f = cob_f[(cob_f["Mes_fin"] >= dr_start) & (cob_f["Mes_fin"] <= dr_end)].copy()
    cob_disp = to_currency(cob_f, ["Cobros_brutos"], currency, fx)

    dev_bruto = fact_rev_f.groupby("Mes_facturacion", as_index=False)["Monto_bruto_hito"].sum().rename(
        columns={"Mes_facturacion": "Mes_fin", "Monto_bruto_hito": "Devengo_bruto"}
    )
    dev_bruto = dev_bruto[(dev_bruto["Mes_fin"] >= dr_start) & (dev_bruto["Mes_fin"] <= dr_end)].copy()
    dev_disp = to_currency(dev_bruto, ["Devengo_bruto"], currency, fx)

    t = pd.DataFrame({"Mes_fin": cal["Mes_fin"]})
    t = t.merge(dev_disp, on="Mes_fin", how="left").merge(cob_disp.rename(columns={"Cobros_brutos": "Cobros"}), on="Mes_fin", how="left")
    t = t[(t["Mes_fin"] >= dr_start) & (t["Mes_fin"] <= dr_end)].fillna(0.0)

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=t["Mes_fin"], y=t["Devengo_bruto"], mode="lines+markers", name="Devengo (bruto)"))
    fig.add_trace(go.Scatter(x=t["Mes_fin"], y=t["Cobros"], mode="lines+markers", name="Cobros (bruto)"))
    fig.update_layout(height=420, xaxis_title="", yaxis_title=("USD" if currency == "USD" else "CLP"))
    st.plotly_chart(fig, use_container_width=True)

    colA, colB = st.columns(2, gap="large")

    with colA:
        st.subheader("Mix: ventas netas por Cliente (click para filtrar)")
        by_cli = fact_rev_f.groupby("Cliente", as_index=False)["Monto_neto_hito"].sum().sort_values("Monto_neto_hito", ascending=False)
        by_cli_disp = to_currency(by_cli, ["Monto_neto_hito"], currency, fx)

        fig_cli = px.bar(by_cli_disp.head(30), x="Cliente", y="Monto_neto_hito", title="Top clientes (neto)")
        fig_cli.update_layout(height=420, xaxis_title="", yaxis_title=("USD" if currency == "USD" else "CLP"))

        if HAS_PLOTLY_EVENTS and st.session_state.click_filters_on:
            ev = plotly_events(fig_cli, click_event=True, hover_event=False, select_event=False, key="evt_client")
            if ev:
                clicked = ev[0].get("x")
                if clicked is not None:
                    st.session_state.clicked_client = str(clicked)
                    st.session_state.clicked_project_label = None
                    st.rerun()
        else:
            st.plotly_chart(fig_cli, use_container_width=True)

    with colB:
        st.subheader("Mix: ventas netas por Proyecto (click para filtrar)")
        by_proj = fact_rev_f.groupby(["Proyecto_ID", "Proyecto_nombre"], as_index=False)["Monto_neto_hito"].sum().sort_values("Monto_neto_hito", ascending=False)
        by_proj["label"] = by_proj["Proyecto_ID"].astype(str) + " — " + by_proj["Proyecto_nombre"].astype(str)
        by_proj_disp = to_currency(by_proj, ["Monto_neto_hito"], currency, fx)

        fig_pr = px.bar(by_proj_disp.head(30), x="label", y="Monto_neto_hito", title="Top proyectos (neto)")
        fig_pr.update_layout(height=420, xaxis_title="", yaxis_title=("USD" if currency == "USD" else "CLP"))
        fig_pr.update_xaxes(tickangle=35)

        if HAS_PLOTLY_EVENTS and st.session_state.click_filters_on:
            ev = plotly_events(fig_pr, click_event=True, hover_event=False, select_event=False, key="evt_project")
            if ev:
                clicked = ev[0].get("x")
                if clicked is not None:
                    st.session_state.clicked_project_label = str(clicked)
                    st.session_state.clicked_client = None
                    st.rerun()
        else:
            st.plotly_chart(fig_pr, use_container_width=True)

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
    cd_m = fact_cd.groupby("Mes_fin", as_index=False)["CD_neto"].sum().rename(columns={"CD_neto": "Costos_directos"})
    opex_m = fact_opex.groupby("Mes_fin", as_index=False)["OPEX_neto"].sum().rename(columns={"OPEX_neto": "OPEX"})
    ppl_m = fact_ppl.groupby("Mes_fin", as_index=False)["Total_personas"].sum().rename(columns={"Total_personas": "Personas"})

    cost = pd.DataFrame({"Mes_fin": cal["Mes_fin"]})
    cost = cost.merge(cd_m, on="Mes_fin", how="left").merge(opex_m, on="Mes_fin", how="left").merge(ppl_m, on="Mes_fin", how="left")
    cost = cost[(cost["Mes_fin"] >= dr_start) & (cost["Mes_fin"] <= dr_end)].fillna(0.0)
    cost_disp = to_currency(cost, ["Costos_directos", "OPEX", "Personas"], currency, fx)

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=cost_disp["Mes_fin"], y=cost_disp["Costos_directos"], stackgroup="one", name="Costos directos"))
    fig.add_trace(go.Scatter(x=cost_disp["Mes_fin"], y=cost_disp["OPEX"], stackgroup="one", name="OPEX"))
    fig.add_trace(go.Scatter(x=cost_disp["Mes_fin"], y=cost_disp["Personas"], stackgroup="one", name="Personas"))
    fig.update_layout(height=420, xaxis_title="", yaxis_title=("USD" if currency == "USD" else "CLP"), title="Stack (neto)")
    st.plotly_chart(fig, use_container_width=True)

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
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(x=iva_disp["Mes_fin"], y=iva_disp["IVA_debito"], mode="lines+markers", name="IVA débito"))
    fig2.add_trace(go.Scatter(x=iva_disp["Mes_fin"], y=iva_disp["IVA_credito"], mode="lines+markers", name="IVA crédito"))
    fig2.add_trace(go.Scatter(x=iva_disp["Mes_fin"], y=iva_disp["IVA_neto_mes"], mode="lines+markers", name="IVA neto mes"))
    fig2.add_trace(go.Scatter(x=iva_disp["Mes_fin"], y=iva_disp["IVA_pago_mes_siguiente"], mode="lines+markers", name="Pago IVA (mes siguiente)"))
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
    scen_names = list(scenarios.keys())
    results = []
    cash_lines = []

    for s in scen_names:
        m = compute_model(excel_bytes, s)
        pyg_s = m["pyg"][(m["pyg"]["Mes_fin"] >= dr_start) & (m["pyg"]["Mes_fin"] <= dr_end)].copy()
        cash_s = m["cash"][(m["cash"]["Mes_fin"] >= dr_start) & (m["cash"]["Mes_fin"] <= dr_end)].copy()
        rev_s = m["fact_rev"][(m["fact_rev"]["Mes_facturacion"] >= dr_start) & (m["fact_rev"]["Mes_facturacion"] <= dr_end)].copy()

        k = build_kpis(pyg_s, cash_s, rev_s)
        k["Escenario"] = s
        results.append(k)

        line = cash_s[["Mes_fin", "Caja_fin_mes"]].copy()
        line["Escenario"] = s
        cash_lines.append(line)

    kdf = pd.DataFrame(results)
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

st.divider()
st.caption("Tip: click-to-filter funciona mejor con 1 clic; para volver, usa 'Limpiar filtros click' o 'Limpiar todo'.")
