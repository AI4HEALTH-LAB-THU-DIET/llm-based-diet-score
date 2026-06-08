#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
03_Interpretability_and_Prediction.py
================================================================================
LLM dietary score: interpretability analysis + prediction model evaluation
Paper figures:
  - Figure 5a: CoT word clouds (extracted from LLM reasoning pipeline; see inference scripts)
  - Figure 5b: LASSO global surrogate model (13 systems + total)
  - Figure 5c: SHAP individual-level explanation (XGBoost surrogate -> SHAP)
  - Figure 5d: SHAP population-level feature importance (mean |SHAP|)
  - Figure 5e: Scaling laws (model size vs C-index)
  - Figure 6a: C-index comparison (LLM vs 7 established indices, Z-test)
  - Figure 6b: Prediction model incremental value (3 nested models)
================================================================================
"""

import os, re, math, warnings
import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.ticker import FixedLocator
from matplotlib.backends.backend_pdf import PdfPages
from pathlib import Path
from math import erf, sqrt
import textwrap

warnings.filterwarnings("ignore")

# ========================= Global Config =========================
DATA_ROOT  = os.path.join("your", "data", "root", "path")
FIG2_DIR   = os.path.join(DATA_ROOT, "results", "Figure2")
FIG4_DIR   = os.path.join(DATA_ROOT, "results", "Figure4")
FIG5_DIR   = os.path.join(DATA_ROOT, "results", "Figure5")
FIG6_DIR   = os.path.join(DATA_ROOT, "results", "Figure6")

for d in [FIG2_DIR, FIG4_DIR, FIG5_DIR, FIG6_DIR]:
    os.makedirs(d, exist_ok=True)

mpl.rcParams.update({
    "pdf.fonttype": 42,
    "ps.fonttype":  42,
    "font.family":  "Arial",
    "axes.edgecolor": "black",
    "axes.linewidth": 1.2,
})

POS_COLOR = "#29A2C6"
NEG_COLOR = "#FF6D31"

# ========================= Utilities =========================
def wrap_label(s, width=24):
    return "\n".join(textwrap.wrap(str(s), width=width, break_long_words=False))

def norm_str(x):
    return " ".join(str(x).lower().split())

# Short food name mapping for LASSO plots
FOOD_SHORT_NAMES = {
    "Muesli": "Muesli",
    "Wholemeal bread": "Wholemeal bread",
    "Tea, decaffeinated": "Tea, decaffeinated",
    "Water (still and sparkling)": "Water",
    "Oat cereal (non sugar)": "Oat cereal (non sugar)",
    "Green leafy/cabbages": "Green leafy/cabbages",
    "Oily fish": "Oily fish",
    "Tea": "Tea",
    "Other vegetables, including mushrooms, fruiting and mixed vegetables": "Mushrooms, fruiting",
    "Full fat yogurt": "Full fat yogurt",
    "Stewed fruit": "Stewed fruit",
    "Bran cereal": "Bran cereal",
    "Raw salad": "Raw salad",
    "Vegetable dips": "Vegetable dips",
    "Unsalted nuts and seeds": "Unsalted nuts and seeds",
    "Berries": "Berries",
    "Apples and pears": "Apples and pears",
    "Dried fruit": "Dried fruit",
    "Low fat yogurt": "Low fat yogurt",
    "Wholemeal pasta, brown rice and other wholegrains": "wholegrains foods",
    "Milk-dairy desserts": "Milk-dairy desserts",
    "Fried/roast potatoes": "Fried/roast potatoes",
    "Pork": "Pork",
    "Pizza": "Pizza",
    "Animal fat spread normal": "Animal fat",
    "Coffee, caffeinated": "Coffee, caffeinated",
    "Breaded/battered fish": "Breaded/battered fish",
    "Breaded/battered chicken": "Breaded/battered chicken",
    "Other sweets": "Other sweets",
    "Savoury snacks": "Savoury snacks",
    "Chocolate confectionery": "Chocolate confectionery",
    "Beef": "Beef",
    "Biscuits": "Biscuits",
    "Sugar-sweetened beverages and other sugary drinks": "Sugar-sweetened beverages",
    "Spirits": "Spirits",
    "Processed meat": "Processed meat",
    "Low/non sugar sugar-sweetened beverages": "Low/non sugar beverages",
    "Beer and cider": "Beer and cider",
    "White bread": "White bread",
}

# 13 physiological system order
SYSTEMS_ORDER = [
    ("blood", "Blood & immune system disorders"),
    ("endocrine", "Endocrine & metabolic disorders"),
    ("mental", "Mental and behavioral disorders"),
    ("nervous", "Nervous system disorders"),
    ("Cataract", "Eye disorders"),
    ("ear", "Ear disorders"),
    ("circulatory", "Circulatory system disorders"),
    ("respiratory", "Respiratory disorders"),
    ("digestive", "Digestive system disorders"),
    ("skin", "Skin disorders"),
    ("musculo", "Musculoskeletal disorders"),
    ("genito", "Genitourinary disorders"),
    ("cancer", "Cancer"),
]

DISEASE_SYSTEMS = [name for _, name in SYSTEMS_ORDER]

PRED_MODELS = [
    {"prefix": "2cindex", "label": "Age + Sex",
     "color": "#4E79A7"},
    {"prefix": "3cindex", "label": "Age + Sex + Lifestyle",
     "color": "#59A14F"},
    {"prefix": "4cindex", "label": "Age + Sex + Lifestyle + LLM score",
     "color": "#9B8DBE"},
]


################################################################################
###### Part 1: LASSO Global Surrogate Model (Fig 5b) ------
################################################################################

def plot_lasso_surrogate_13systems(input_file=None, output_pdf=None, output_png=None):
    """
    13-system x Top40 food LASSO coefficient bar chart, 4-column grid.
    input_file: Excel with one sheet per system (sheet names must match SYSTEMS_ORDER).
    """
    if input_file is None:
        input_file = os.path.join(FIG5_DIR, "Lasso_各系统结果.xlsx")
    if output_pdf is None:
        output_pdf = os.path.join(FIG5_DIR, "Fig5b_LASSO_surrogate_13systems.pdf")
    if output_png is None:
        output_png = os.path.join(FIG5_DIR, "Fig5b_LASSO_surrogate_13systems.png")

    all_panels = []
    global_max_abs = 0

    for sheet_name, title in SYSTEMS_ORDER:
        df = pd.read_excel(input_file, sheet_name=sheet_name)
        df = df.dropna(subset=["Feature", "Coefficient", "Abs_Coeff"]).copy()
        df["ShortFeature"] = df["Feature"].map(FOOD_SHORT_NAMES).fillna(df["Feature"])

        top40 = df.sort_values("Abs_Coeff", ascending=False).head(40).copy()
        top40["sign_group"] = np.where(top40["Coefficient"] >= 0, 0, 1)
        top40["sort_val"] = np.where(
            top40["Coefficient"] >= 0, -top40["Coefficient"], top40["Coefficient"])
        top40 = top40.sort_values(["sign_group", "sort_val"], ascending=[True, True])
        top40["PlotLabel"] = top40["ShortFeature"].apply(lambda x: wrap_label(x, width=24))

        global_max_abs = max(global_max_abs, top40["Coefficient"].abs().max())
        all_panels.append((title, top40))

    xlim = global_max_abs * 1.10
    n_panels = len(all_panels)
    ncols = 4
    nrows = math.ceil(n_panels / ncols)

    fig, axes = plt.subplots(nrows=nrows, ncols=ncols,
                             figsize=(24, nrows * 10), constrained_layout=True)
    axes = np.array(axes).reshape(-1)

    for ax, (title, dat) in zip(axes, all_panels):
        y = np.arange(len(dat))
        colors = [POS_COLOR if v >= 0 else NEG_COLOR for v in dat["Coefficient"]]
        ax.barh(y, dat["Coefficient"], color=colors, edgecolor="none", height=0.8)
        ax.set_yticks(y)
        ax.set_yticklabels(dat["PlotLabel"], fontsize=8)
        ax.axvline(0, color="black", linewidth=0.8)
        ax.set_xlim(-xlim, xlim)
        ax.invert_yaxis()
        ax.set_title(title, fontsize=13, pad=10)
        ax.grid(axis="x", linestyle="--", linewidth=0.4, alpha=0.35)
        ax.set_axisbelow(True)
        ax.tick_params(axis="x", labelsize=9)
        for spine in ["top", "right"]:
            ax.spines[spine].set_visible(False)

    for ax in axes[len(all_panels):]:
        ax.axis("off")

    for i, ax in enumerate(axes):
        if i // ncols == nrows - 1:
            ax.set_xlabel("Coefficient", fontsize=10)

    fig.suptitle("Top 40 major foods across disease systems", fontsize=18)
    fig.savefig(output_png, dpi=300, bbox_inches="tight")
    with PdfPages(output_pdf) as pdf:
        pdf.savefig(fig, bbox_inches="tight")
    plt.close(fig)
    print(f"[Fig 5b] 13-system LASSO: {output_pdf}")


def plot_lasso_surrogate_total(input_file=None, output_pdf=None):
    """Total score LASSO coefficients. input_file must have Feature, Coefficient columns."""
    if input_file is None:
        input_file = os.path.join(FIG5_DIR, "Lasso_total_overall_score.xlsx")
    if output_pdf is None:
        output_pdf = os.path.join(FIG5_DIR, "Fig5b_LASSO_total_overall.pdf")

    try:
        df = pd.read_excel(input_file)
    except Exception:
        try:
            df = pd.read_csv(input_file, encoding='gbk')
        except Exception:
            df = pd.read_csv(input_file, encoding='ISO-8859-1')

    df['Abs_Coeff'] = df['Coefficient'].abs()
    df = df.sort_values(by='Abs_Coeff', ascending=True)

    plt.figure(figsize=(10, max(6, len(df) * 0.45 + 1)))
    colors = ['#4DBBD5' if x < 0 else '#E64B35' for x in df['Coefficient']]
    plt.barh(df['Feature'], df['Coefficient'], color=colors, height=0.6, alpha=0.9)
    plt.axvline(x=0, color='black', linewidth=0.8)
    plt.xlabel('Coefficient Value', fontsize=12, fontweight='bold')
    plt.title('Feature Importance (LASSO Surrogate - Overall)', fontsize=14, fontweight='bold', pad=20)
    plt.gca().spines['top'].set_visible(False)
    plt.gca().spines['right'].set_visible(False)
    plt.yticks(fontstyle='italic', fontsize=11)

    if 'Significance' in df.columns:
        max_val = df['Abs_Coeff'].max()
        offset = max_val * 0.02
        for i, (coef, sig) in enumerate(zip(df['Coefficient'], df['Significance'])):
            if pd.isna(sig):
                continue
            pos = coef + offset if coef >= 0 else coef - offset
            ha = 'left' if coef >= 0 else 'right'
            plt.text(pos, i, str(sig), va='center', ha=ha, fontsize=10, color='black')

    plt.tight_layout()
    plt.savefig(output_pdf, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"[Fig 5b] Total LASSO: {output_pdf}")


################################################################################
###### Part 2: SHAP Feature Importance (Fig 5c-d) ------
################################################################################

def plot_shap_population(input_file=None, output_pdf=None, top_n=30):
    """
    Fig 5d: SHAP population-level feature importance (mean |SHAP| across all samples).
    input_file: CSV/XLSX with Feature, SHAP_mean, SHAP_std columns.
    """
    if input_file is None:
        input_file = os.path.join(FIG5_DIR, "SHAP_population_importance.csv")
    if output_pdf is None:
        output_pdf = os.path.join(FIG5_DIR, "Fig5d_SHAP_population.pdf")

    try:
        df = pd.read_excel(input_file)
    except Exception:
        df = pd.read_csv(input_file)

    df = df.sort_values("SHAP_mean", ascending=True).tail(top_n)

    fig, ax = plt.subplots(figsize=(6, max(5, top_n * 0.35)))
    colors = ['#d68a00' if x > 0 else '#3A68B4' for x in df['SHAP_mean']]

    ax.barh(np.arange(len(df)), df['SHAP_mean'], xerr=df.get('SHAP_std', None),
            color=colors, height=0.7, alpha=0.8, capsize=2)
    ax.set_yticks(np.arange(len(df)))
    ax.set_yticklabels(df['Feature'], fontsize=10)
    ax.axvline(x=0, color='black', linewidth=0.8, alpha=0.5)
    ax.set_xlabel("Mean |SHAP| value", fontsize=12)
    ax.set_title("SHAP feature importance (population level)", fontsize=14, pad=12)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    fig.tight_layout()
    fig.savefig(output_pdf, dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"[Fig 5d] SHAP population: {output_pdf}")


def plot_shap_individual(input_file=None, output_pdf=None, output_png=None):
    """
    Fig 5c: SHAP individual-level explanation (single participant food contributions).
    input_file: CSV/XLSX with Food, Amount, SHAP columns.
    """
    if input_file is None:
        input_file = os.path.join(FIG5_DIR, "SHAP_individual_example.csv")
    if output_pdf is None:
        output_pdf = os.path.join(FIG5_DIR, "Fig5c_SHAP_individual.pdf")
    if output_png is None:
        output_png = os.path.join(FIG5_DIR, "Fig5c_SHAP_individual.png")

    try:
        df = pd.read_excel(input_file)
    except Exception:
        df = pd.read_csv(input_file)

    df['Label'] = df['Food'].astype(str) + " = " + df['Amount'].astype(str)
    df = df.sort_values(by='SHAP', ascending=True)

    plt.figure(figsize=(5, max(10, len(df) * 0.45)))
    colors = ['#d68a00' if x > 0 else '#3A68B4' for x in df['SHAP']]
    plt.barh(df['Label'], df['SHAP'], color=colors, height=0.7, alpha=0.7)
    plt.axvline(x=0, color='black', linewidth=0.8, linestyle='-', alpha=0.5)
    plt.xlabel("SHAP value (contribution to prediction)", fontsize=12)
    plt.title("SHAP values for a single participant", fontsize=14, pad=20)
    plt.gca().spines['top'].set_visible(False)
    plt.gca().spines['right'].set_visible(False)
    plt.gca().spines['left'].set_visible(False)
    plt.tick_params(axis='y', labelsize=10, left=False)
    plt.tick_params(axis='x', labelsize=10)
    plt.tight_layout()

    plt.savefig(output_pdf, dpi=300, bbox_inches='tight')
    plt.savefig(output_png, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"[Fig 5c] SHAP individual: {output_pdf}")


################################################################################
###### Part 3: Scaling Laws (Fig 5e) ------
################################################################################

def plot_scaling_laws(input_file=None, output_pdf=None):
    """
    Fig 5e: Model size vs C-index (Qwen and Llama families).
    input_file: CSV/XLSX with model, param_count, c_index, ci_lower, ci_upper, model_family.
    """
    if input_file is None:
        input_file = os.path.join(FIG5_DIR, "scaling_laws_results.csv")
    if output_pdf is None:
        output_pdf = os.path.join(FIG5_DIR, "Fig5e_scaling_laws.pdf")

    try:
        data = pd.read_excel(input_file)
    except Exception:
        data = pd.read_csv(input_file)

    family_colors = {"Qwen": "#E64B35", "Llama": "#4DBBD5"}
    families = data['model_family'].unique()

    fig, ax = plt.subplots(figsize=(7, 5))

    for fam in families:
        sub = data[data['model_family'] == fam].sort_values('param_count')
        color = family_colors.get(fam, '#333333')
        ax.errorbar(sub['param_count'], sub['c_index'],
                    yerr=[sub['c_index'] - sub['ci_lower'],
                          sub['ci_upper'] - sub['c_index']],
                    fmt='o-', color=color, linewidth=2, markersize=10,
                    capsize=5, capthick=1.5, elinewidth=1.5, label=fam)
        for _, row in sub.iterrows():
            ax.annotate(row['model'],
                        (row['param_count'], row['c_index']),
                        textcoords="offset points", xytext=(8, -14),
                        fontsize=9, color=color)

    ax.set_xscale('log')
    ax.set_xlabel('Model parameters (log scale)', fontsize=13)
    ax.set_ylabel('C-index for mortality prediction', fontsize=13)
    ax.set_title('Scaling law: Model size vs dietary assessment performance',
                 fontsize=14, fontweight='bold')
    ax.legend(frameon=False, fontsize=10)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.grid(axis='y', linestyle='--', alpha=0.3)

    fig.tight_layout()
    fig.savefig(output_pdf, dpi=300, bbox_inches='tight')
    plt.close(fig)
    print(f"[Fig 5e] Scaling laws: {output_pdf}")


################################################################################
###### Part 4: C-index Statistical Comparison (Fig 6a) ------
################################################################################

def parse_cindex_ci(s):
    """Parse '0.514 (0.507-0.520)' format."""
    if pd.isna(s):
        return np.nan, np.nan, np.nan
    s = str(s).strip()
    m = re.match(r"^\s*([0-9.]+)\s*\(\s*([0-9.]+)\s*[–-]\s*([0-9.]+)\s*\)\s*$", s)
    if not m:
        return np.nan, np.nan, np.nan
    return tuple(map(float, m.groups()))


def ci_to_se(lower, upper):
    if pd.isna(lower) or pd.isna(upper):
        return np.nan
    return (upper - lower) / (2 * 1.96)


def norm_cdf(x):
    return 0.5 * (1 + erf(x / sqrt(2)))


def two_sided_p_from_z(z):
    return 2 * (1 - norm_cdf(abs(z)))


def compare_two_cindex_approx(ref_text, other_text):
    """Approximate Z-test for two independent C-index values."""
    ref_mean, ref_lo, ref_up = parse_cindex_ci(ref_text)
    oth_mean, oth_lo, oth_up = parse_cindex_ci(other_text)

    if any(pd.isna(x) for x in [ref_mean, ref_lo, ref_up, oth_mean, oth_lo, oth_up]):
        return np.nan, np.nan, np.nan

    ref_se = ci_to_se(ref_lo, ref_up)
    oth_se = ci_to_se(oth_lo, oth_up)
    if pd.isna(ref_se) or pd.isna(oth_se):
        return np.nan, np.nan, np.nan

    denom = np.sqrt(ref_se ** 2 + oth_se ** 2)
    if denom == 0:
        return np.nan, np.nan, np.nan

    diff = oth_mean - ref_mean
    z = diff / denom
    p = two_sided_p_from_z(z)
    return p, z, diff


def format_p(p):
    if pd.isna(p):
        return np.nan
    if p < 0.001:
        return "<0.001"
    return f"{p:.3f}"


def run_cindex_comparison(input_xlsx=None, output_xlsx=None,
                          ref_col="total__overall", sheet_name="Cindex"):
    """
    Fig 6a: LLM score vs each established index C-index, approximate Z-test per outcome.
    """
    if input_xlsx is None:
        input_xlsx = os.path.join(FIG6_DIR, "Cindex_all_scores.xlsx")
    if output_xlsx is None:
        output_xlsx = os.path.join(FIG6_DIR, "Fig6a_Cindex_P_values.xlsx")

    print("=" * 60)
    print("[Fig 6a] C-index Statistical Comparison")

    xlsx = pd.ExcelFile(input_xlsx)
    df = pd.read_excel(input_xlsx, sheet_name=sheet_name)

    if ref_col not in df.columns:
        raise ValueError(f"Reference column {ref_col} not found. Available: {df.columns.tolist()}")

    meta_cols = [c for c in ["disease", "N", "Events"] if c in df.columns]
    score_cols = [c for c in df.columns if c not in meta_cols]

    new_cols = meta_cols.copy()
    for col in score_cols:
        new_cols.append(col)
        if col != ref_col:
            new_cols.append(f"{col}__P_vs_LLM")

    out_df = pd.DataFrame(index=df.index)
    for c in meta_cols:
        out_df[c] = df[c]

    for col in score_cols:
        out_df[col] = df[col]
        if col != ref_col:
            pvals = []
            for i in range(len(df)):
                p, _, _ = compare_two_cindex_approx(
                    df.loc[i, ref_col], df.loc[i, col])
                pvals.append(format_p(p))
            out_df[f"{col}__P_vs_LLM"] = pvals

    out_df = out_df[new_cols]

    # Long-format table
    long_records = []
    for _, row in df.iterrows():
        disease = row.get("disease", None)
        for col in score_cols:
            if col == ref_col:
                continue
            p, z, diff = compare_two_cindex_approx(row[ref_col], row[col])
            long_records.append({
                "disease": disease, "ref_score": ref_col,
                "other_score": col, "diff_other_minus_ref": diff,
                "z_value": z, "p_value": p, "p_display": format_p(p),
            })
    long_df = pd.DataFrame(long_records)

    with pd.ExcelWriter(output_xlsx, engine="openpyxl") as writer:
        out_df.to_excel(writer, sheet_name="Cindex_with_P", index=False)
        df.to_excel(writer, sheet_name="Cindex_original", index=False)
        long_df.to_excel(writer, sheet_name="P_long", index=False)

    print(f"  P-values saved -> {output_xlsx}")
    return out_df, long_df


################################################################################
###### Part 5: Prediction Model Incremental Value (Fig 6b) ------
################################################################################

def _load_prediction_data(xlsx_path, mapping_csv=None):
    if xlsx_path.endswith('.csv'):
        df_raw = pd.read_csv(xlsx_path)
    else:
        try:
            df_raw = pd.read_excel(xlsx_path)
        except Exception:
            df_raw = pd.read_csv(xlsx_path)

    # Long -> wide
    if 'model' in df_raw.columns and 'disease' in df_raw.columns:
        model_map = {
            'M2_age_sex': '2cindex',
            'M3_age_sex_health': '3cindex',
            'M4_full': '4cindex',
        }
        df_raw['model_prefix'] = df_raw['model'].map(model_map)
        df_wide = df_raw.pivot(
            index='disease', columns='model_prefix',
            values=['cindex_mean', 'cindex_std'])
        df_wide.columns = [f"{col[1]}_{col[0].split('_')[1]}"
                           for col in df_wide.columns]
        df = df_wide.reset_index()
    else:
        df = df_raw
        if "disease" not in df.columns:
            df.rename(columns={df.columns[0]: "disease"}, inplace=True)

    # Disease name mapping
    mapping = {}
    if mapping_csv and os.path.exists(mapping_csv):
        mp = pd.read_csv(mapping_csv)
        mapping = dict(zip(
            mp.iloc[:, 0].astype(str).str.strip(),
            mp.iloc[:, -1].astype(str).str.strip()))
    df["disease_clean"] = df["disease"].astype(str).str.strip().map(mapping)
    fallback = df["disease"].astype(str).str.replace("_", " ", regex=False).str.title()
    df["disease_clean"] = df["disease_clean"].fillna(fallback)
    return df


def _draw_cindex_panel(ax, row):
    pts = []
    for i, m in enumerate(PRED_MODELS):
        mean = row.get(f"{m['prefix']}_mean")
        std = row.get(f"{m['prefix']}_std")
        if pd.notna(mean) and pd.notna(std):
            pts.append(dict(x=i, mean=float(mean),
                            err=1.96 * float(std), color=m["color"]))
    if not pts:
        ax.axis("off")
        return
    ax.set_xlim(-0.6, len(PRED_MODELS) - 0.4)
    for p in pts:
        ax.hlines(p["mean"], -0.6, p["x"], colors=p["color"],
                  ls="--", lw=0.8, alpha=0.5)
        ax.errorbar(p["x"], p["mean"], yerr=p["err"],
                    fmt="none", ecolor=p["color"], lw=1.2, capsize=3)
        ax.scatter(p["x"], p["mean"], s=60, c=p["color"])
    lo = min(p["mean"] - p["err"] for p in pts)
    hi = max(p["mean"] + p["err"] for p in pts)
    pad = max(0.01, (hi - lo) * 0.12)
    ax.set_ylim(max(0, lo - pad), min(1, hi + pad))
    ax.tick_params(axis="y", labelsize=8, width=1)
    ax.set_xticks(range(len(PRED_MODELS)))
    ax.set_xticklabels([""] * len(PRED_MODELS))
    ax.set_title(row["disease_clean"], fontsize=9, fontweight="bold")


def _make_prediction_figure(df, out_path, layout):
    n = len(df)
    if n == 0:
        print(f"[{out_path}] No data, skipping.")
        return
    ncols = layout["ncols"]
    nrows = int(np.ceil(n / ncols))
    fig, axs = plt.subplots(nrows, ncols,
                            figsize=(layout["fw"] * ncols, layout["fh"] * nrows))
    if n == 1:
        axs = np.array([axs])
    axs = np.array(axs).reshape(-1)
    for i, ax in enumerate(axs):
        if i < n:
            _draw_cindex_panel(ax, df.iloc[i])
        else:
            ax.axis("off")
    fig.text(0.01, 0.5, "C-index", rotation=90,
             fontsize=12, fontweight="bold", va="center")
    handles = [Line2D([0], [0], marker='o', color='w',
                      markerfacecolor=m["color"], label=m["label"])
               for m in PRED_MODELS]
    fig.legend(handles=handles, loc="lower center", ncol=3,
               frameon=False, fontsize=10)
    plt.subplots_adjust(left=0.05, right=0.995, top=0.97,
                        bottom=layout["bp"],
                        wspace=layout["ws"], hspace=layout["hs"])
    fig.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close(fig)


def run_prediction_model_plots(input_xlsx=None, mapping_csv=None, output_dir=None):
    """
    Fig 6b: 3 nested models (Age+Sex -> +Lifestyle -> +LLM score) C-index forest-panel.
    Outputs: All / System diseases / Non-system diseases (3 PDFs).
    """
    if input_xlsx is None:
        input_xlsx = os.path.join(FIG6_DIR, "prediction_cindex.xlsx")
    if mapping_csv is None:
        mapping_csv = os.path.join(DATA_ROOT, "disease_mapping.csv")
    if output_dir is None:
        output_dir = FIG6_DIR

    print("=" * 60)
    print("[Fig 6b] Prediction Model Incremental Value")

    df = _load_prediction_data(input_xlsx, mapping_csv)

    sys_order = {norm_str(name): i for i, name in enumerate(DISEASE_SYSTEMS)}
    mask_sys = df["disease_clean"].apply(lambda x: norm_str(x) in sys_order)
    df_sys = df[mask_sys].copy()
    df_non = df[~mask_sys].copy()
    df_sys["sort_key"] = df_sys["disease_clean"].apply(
        lambda x: sys_order.get(norm_str(x), 999))
    df_sys = df_sys.sort_values("sort_key").drop(columns=["sort_key"])

    layout_all = dict(ncols=6, fw=2.0, fh=1.5, ws=0.30, hs=0.25, bp=0.05)
    layout_sys = dict(ncols=5, fw=2.3, fh=2.0, ws=0.28, hs=0.30, bp=0.08)
    layout_non = dict(ncols=6, fw=2.0, fh=1.5, ws=0.30, hs=0.25, bp=0.05)

    _make_prediction_figure(df, os.path.join(output_dir, "Fig6b_All.pdf"), layout_all)
    _make_prediction_figure(df_sys, os.path.join(output_dir, "Fig6b_Systems.pdf"), layout_sys)
    _make_prediction_figure(df_non, os.path.join(output_dir, "Fig6b_Non_systems.pdf"), layout_non)

    print(f"  Saved to {output_dir}/Fig6b_*.pdf")


################################################################################
###### Part 6: HR Summary Table (Fig 2a supplement) ------
################################################################################

def summarize_hr_results(folder_path=None, output_file=None):
    """Cross-score HR results -> multi-sheet Excel summary."""
    import glob

    if folder_path is None:
        folder_path = os.path.join(FIG2_DIR, "HR_results")
    if output_file is None:
        output_file = os.path.join(FIG2_DIR, "Fig2a_HR_summary.xlsx")

    print("=" * 60)
    print("[Fig 2a] HR Summary Table")

    files = (glob.glob(os.path.join(folder_path, "*.xlsx")) +
             glob.glob(os.path.join(folder_path, "*.csv")))
    if not files:
        print(f"  No files found: {folder_path}")
        return

    all_data = []
    for f in files:
        try:
            df = pd.read_csv(f) if f.endswith('.csv') else pd.read_excel(f)
            df.columns = [c.strip() for c in df.columns]

            target_cols = ['disease', 'HR', 'CI95_low', 'CI95_high', 'p_value', 'FDR_P']
            if all(col in df.columns for col in target_cols):
                extracted = df[target_cols].copy()
            else:
                extracted = df.iloc[:, [1, 8, 9, 10, 11, 12]].copy()
            extracted.columns = ['Disease', 'HR', 'CI_low', 'CI_high', 'P_value', 'FDRp']
            extracted['Source'] = os.path.basename(f)
            all_data.append(extracted)
        except Exception as e:
            print(f"  Error processing {f}: {e}")

    if not all_data:
        return

    combined = pd.concat(all_data, ignore_index=True)
    for c in ['HR', 'CI_low', 'CI_high', 'P_value', 'FDRp']:
        combined[c] = pd.to_numeric(combined[c], errors='coerce')
    combined = combined.dropna(subset=['Disease', 'HR'])

    hr_pivot = combined.pivot_table(index='Disease', columns='Source', values='HR')
    ci_low  = combined.pivot_table(index='Disease', columns='Source', values='CI_low')
    ci_high = combined.pivot_table(index='Disease', columns='Source', values='CI_high')
    fdr_pivot = combined.pivot_table(index='Disease', columns='Source', values='FDRp')

    ci_fmt = hr_pivot.apply(lambda row: pd.Series({
        col: f"({ci_low.loc[row.name, col]:.2f}-{ci_high.loc[row.name, col]:.2f})"
        if pd.notna(ci_low.loc[row.name, col]) else ""
        for col in hr_pivot.columns}), axis=1)

    hr_fdr_fmt = hr_pivot.apply(lambda row: pd.Series({
        col: f"{hr_pivot.loc[row.name, col]:.2f} ({fdr_pivot.loc[row.name, col]:.2e})"
        if pd.notna(hr_pivot.loc[row.name, col]) else ""
        for col in hr_pivot.columns}), axis=1)

    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        hr_pivot.to_excel(writer, sheet_name='1_HR_summary')
        ci_fmt.to_excel(writer, sheet_name='2_95%CI_summary')
        fdr_pivot.to_excel(writer, sheet_name='3_FDR_summary')
        hr_fdr_fmt.to_excel(writer, sheet_name='4_HR_FDR_combined')

    print(f"  HR summary -> {output_file}")


################################################################################
###### Part 7: Mediation Bubble Plot (Fig 4d supplement) ------
################################################################################

def plot_mediation_bubble(med_csv=None, cox_csv=None,
                          output_pdf=None, output_png=None, top_n=25):
    """
    Mediation bubble plot (reads from R mediation output).
    med_csv: mediation_weibull_*.csv (Protein, Label, ACME, PropMed)
    cox_csv: cox_screen_*.csv (Protein, HR)
    """
    if med_csv is None:
        med_csv = os.path.join(FIG4_DIR, "mediation_weibull_death.csv")
    if cox_csv is None:
        cox_csv = os.path.join(FIG4_DIR, "cox_screen_death.csv")
    if output_pdf is None:
        output_pdf = os.path.join(FIG4_DIR, "Fig4d_mediation_bubble.pdf")
    if output_png is None:
        output_png = os.path.join(FIG4_DIR, "Fig4d_mediation_bubble.png")

    print("[Fig 4d] Mediation Bubble Plot")

    med = pd.read_csv(med_csv)
    cox = pd.read_csv(cox_csv)

    med["_rank"] = pd.to_numeric(med["PropMed"], errors="coerce").abs()
    top = med.sort_values("_rank", ascending=False).head(top_n).copy()
    df = top.merge(cox[["Protein", "HR"]], on="Protein", how="left")
    df = df.dropna(subset=["HR", "ACME", "PropMed", "Label"])
    df = df.sort_values("HR", ascending=False).copy()

    hr = df["HR"].to_numpy()
    acme = pd.to_numeric(df["ACME"], errors="coerce").to_numpy()
    pm_raw = pd.to_numeric(df["PropMed"], errors="coerce").abs().to_numpy()

    def _winsorize(x, q_low=0.05, q_high=0.95):
        x = np.asarray(x, dtype=float)
        lo = np.nanquantile(x, q_low)
        hi = np.nanquantile(x, q_high)
        return np.clip(x, lo, hi), lo, hi

    def _scale_sizes(v, out_min=70, out_max=500, power=1.7):
        v = np.asarray(v, dtype=float); v = np.clip(v, 0, None)
        if np.nanmax(v) == np.nanmin(v):
            return np.full_like(v, (out_min + out_max) / 2.0, dtype=float)
        vmin, vmax = np.nanmin(v), np.nanmax(v)
        z = np.power((v - vmin) / (vmax - vmin + 1e-12), power)
        return out_min + z * (out_max - out_min)

    pm_clip, pm_lo, pm_hi = _winsorize(pm_raw)
    sizes = _scale_sizes(pm_clip)
    colors = np.where(acme >= 0, "#D64B3B", "#2E6FBB")

    XMIN, XMAX = 0.9, 2.5
    fig, ax = plt.subplots(figsize=(5.0, max(5, top_n * 0.28)), dpi=300)
    ax.axvline(1.0, linestyle="--", linewidth=1.6, color="#666666", alpha=0.95, zorder=1)

    y = np.arange(len(df))
    for yi, xi, c in zip(y, hr, colors):
        xi_plot = min(max(xi, XMIN), XMAX)
        ax.hlines(yi, XMIN, xi_plot, color=c, linewidth=2.0, alpha=0.70, zorder=2)

    hr_plot = np.clip(hr, XMIN, XMAX)
    ax.scatter(hr_plot, y, s=sizes, c=colors, edgecolor="white",
               linewidth=1.0, alpha=0.95, zorder=3)

    ax.set_yticks(y)
    ax.set_yticklabels(df["Label"].to_list(), fontsize=11)
    ax.invert_yaxis()
    ax.set_xlim(XMIN, XMAX)
    ax.xaxis.set_major_locator(FixedLocator([1.0, 1.5, 2.0, 2.5]))
    ax.set_xlabel("Hazard Ratio (HR)", fontsize=12)
    ax.set_title("Mediation: LLM dietary score -> all-cause mortality",
                 fontsize=14, fontweight="bold", pad=10)

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_linewidth(1.6)
    ax.spines["bottom"].set_linewidth(1.6)
    ax.tick_params(direction="out", length=6, width=1.4, labelsize=11)
    ax.grid(False)

    # Size legend
    PM_LEGEND = [0.15, 0.20, 0.25]
    pm_leg = np.clip(np.array(PM_LEGEND, dtype=float), pm_lo, pm_hi)
    pm_leg_sizes = _scale_sizes(pm_leg)
    size_handles = [
        Line2D([0], [0], marker="o", color="none",
               markerfacecolor="#9E9E9E", markeredgecolor="white",
               markeredgewidth=1.0, markersize=np.sqrt(s),
               label=f"{int(v*100)}%")
        for v, s in zip(PM_LEGEND, pm_leg_sizes)]
    ax.legend(handles=size_handles, title="Proportion mediated",
              frameon=False, loc="center right")

    plt.tight_layout()
    fig.savefig(output_pdf, bbox_inches="tight")
    fig.savefig(output_png, bbox_inches="tight")
    plt.close(fig)
    print(f"[Fig 4d] {output_pdf}")


################################################################################
###### Main Entry ------
################################################################################

if __name__ == "__main__":
    print("=" * 60)
    print("03_Interpretability_and_Prediction.py")
    print("=" * 60)

    # ---- Fig 5b: LASSO global surrogate ----
    plot_lasso_surrogate_13systems()
    plot_lasso_surrogate_total()

    # ---- Fig 5c-d: SHAP (individual + population) ----
    plot_shap_individual()   # Fig 5c
    plot_shap_population()   # Fig 5d

    # ---- Fig 5e: Scaling Laws ----
    plot_scaling_laws()

    # ---- Fig 6a: C-index comparison ----
    run_cindex_comparison()

    # ---- Fig 6b: Prediction model incremental value ----
    run_prediction_model_plots()

    # ---- Fig 4d: Mediation bubble ----
    plot_mediation_bubble()

    # ---- Fig 2a HR summary ----
    summarize_hr_results()

    print("\n===== 03_Interpretability_and_Prediction.py done =====")
