################################################################################
# 02_Proteomics_Analysis.R
# ============================================================================
# Plasma proteomics pipeline (UKB Olink, ~2,900 proteins)
# Paper: Figure 4a-f
# Reference style: MainAnalysis.R
################################################################################

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(readr)
  library(readxl)
  library(haven)
  library(tibble)
  library(limma)
  library(ggplot2)
  library(ggrepel)
  library(glmnet)
  library(survival)
  library(survcomp)
  library(caret)
  library(patchwork)
  library(cowplot)
  library(future)
  library(future.apply)
  library(progressr)
  library(clusterProfiler)
  library(org.Hs.eg.db)
  library(enrichplot)
  library(GseaVis)
  library(mediation)
  library(broom)
  library(scales)
  library(grid)
  library(ggforce)
  library(ggprism)
  library(UpSetR)
})

if (requireNamespace("conflicted", quietly = TRUE)) {
  conflicted::conflicts_prefer(dplyr::filter, dplyr::lag, dplyr::select)
}

###### Global Parameters ------
set.seed(2024)

DATA_ROOT  <- "your/data/root/path"
path_protein    <- file.path(DATA_ROOT, "proteomics", "Proteomics_0.dta")
path_score      <- file.path(DATA_ROOT, "scores", "LLM_score.xlsx")
path_covars     <- file.path(DATA_ROOT, "covariates_mice_imputed+fastime.csv")
map_path        <- file.path(DATA_ROOT, "proteomics", "coding143.tsv")
disease_file    <- file.path(DATA_ROOT, "totaldisease.csv")
OUT_DIR         <- file.path(DATA_ROOT, "results", "proteomics")

dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)

### Analysis parameters
score_col       <- "overall diet score"
top_frac        <- 0.10
miss_thr        <- 0.20
lfc_thr         <- 0.20
fdr_thr         <- 0.05
protein_regex   <- "^p\\d+"
UNI_FDR_THR     <- 0.05
ALPHA_GRID      <- seq(0, 1, by = 0.1)
NFOLDS_EN       <- 5
KFOLDS          <- 10

COVARS_CANDIDATES <- c("age", "sex", "BMI", "energy", "income",
                       "ethnicity", "education", "smoking", "alcohol", "Fast_dur")
SIG_FDR_THR     <- 0.05

read_any <- function(path) {
  ext <- tolower(tools::file_ext(path))
  if (ext %in% c("csv", "txt")) return(suppressMessages(readr::read_csv(path, show_col_types = FALSE)))
  if (ext %in% c("xlsx", "xls")) return(suppressMessages(readxl::read_excel(path)))
  stop("Only csv/xlsx supported")
}


################################################################################
###### Part 1: Data Loading ------
################################################################################

score_col_sym <- rlang::sym(score_col)
score_tbl <- readxl::read_excel(path_score) %>%
  dplyr::select(n_eid, !!score_col_sym) %>%
  dplyr::rename(eid = n_eid) %>%
  dplyr::mutate(eid = as.numeric(eid))

dat_protein <- haven::read_dta(path_protein) %>% as_tibble()

covars <- readr::read_csv(path_covars, show_col_types = FALSE) %>%
  as_tibble() %>%
  dplyr::rename(eid = n_eid)

mapping <- readr::read_tsv(map_path, show_col_types = FALSE) %>%
  dplyr::mutate(
    protein_id   = paste0("p", coding),
    gene_symbol  = sub(";.*$", "", meaning),
    protein_desc = sub("^[^;]*;\\s*", "", meaning)
  ) %>%
  dplyr::distinct(protein_id, .keep_all = TRUE)

################################################################################
###### Part 2: limma Differential Expression + Volcano (Fig 4a) ------
################################################################################

### Merge data + assign diet groups
score_tbl$eid <- as.numeric(score_tbl$eid)
protein_cols  <- names(dat_protein)[grepl(protein_regex, names(dat_protein))]

dat <- score_tbl %>%
  dplyr::inner_join(dat_protein, by = "eid") %>%
  dplyr::inner_join(covars, by = "eid")

dat <- dat %>%
  dplyr::mutate(across(all_of(protein_cols), ~ as.numeric(scale(.x))))

dat <- dat %>%
  dplyr::arrange(.data[[score_col]]) %>%
  dplyr::mutate(
    row_id = dplyr::row_number(),
    n      = dplyr::n(),
    cut_n  = floor(top_frac * n),
    DietGroup = dplyr::case_when(
      row_id <= cut_n     ~ "Bottom10",
      row_id >  n - cut_n ~ "Top10",
      TRUE                ~ NA_character_
    )
  ) %>%
  dplyr::filter(!is.na(.data$DietGroup)) %>%
  dplyr::mutate(DietGroup = factor(.data$DietGroup, levels = c("Bottom10", "Top10")))

dat <- dat[, !(names(dat) %in% c("row_id", "n", "cut_n"))]

### Missingness filter + median imputation
protein_cols <- names(dat)[grepl(protein_regex, names(dat))]
keep_base <- intersect(c("eid", score_col, "DietGroup", COVARS_CANDIDATES), names(dat))
dat_sub <- dat %>% dplyr::select(dplyr::any_of(keep_base), dplyr::all_of(protein_cols))

miss_rate <- colMeans(is.na(dat_sub[, protein_cols, drop = FALSE]))
keep_proteins <- names(miss_rate[miss_rate <= miss_thr])
dat_sub <- dat_sub %>%
  dplyr::mutate(across(all_of(keep_proteins), ~ replace_na(., median(., na.rm = TRUE))))

### limma differential expression
X <- t(as.matrix(dat_sub[, keep_proteins, drop = FALSE]))

cov_in_model <- intersect(c("age", "sex", "BMI", "energy", "income",
                            "ethnicity", "education", "smoking", "alcohol"),
                          names(dat_sub))
formula_str <- paste0("~ 0 + DietGroup + ", paste(cov_in_model, collapse = " + "))
des <- model.matrix(as.formula(formula_str), data = dat_sub)
colnames(des) <- make.names(colnames(des))

fit  <- lmFit(X, des)
fit2 <- contrasts.fit(fit, makeContrasts(DietGroupTop10 - DietGroupBottom10, levels = des))
fit2 <- eBayes(fit2)

results <- topTable(fit2, adjust = "BH", number = Inf) %>%
  rownames_to_column("protein_id")

### Annotate + volcano plot
min_p_value <- 5e-323
res_tbl <- results %>%
  dplyr::mutate(
    adj.P.Val       = pmax(adj.P.Val, min_p_value),
    Minus_Log10_FDR = -log10(adj.P.Val),
    Significance = case_when(
      logFC > 0 & adj.P.Val < fdr_thr  ~ "Upregulated",
      logFC < 0 & adj.P.Val < fdr_thr  ~ "Downregulated",
      TRUE                             ~ "Not Significant"
    )
  ) %>%
  dplyr::left_join(
    mapping %>% dplyr::select(protein_id, gene_symbol, protein_desc),
    by = "protein_id"
  )

top_n <- 20; bottom_n <- 20
label_df <- dplyr::bind_rows(
  res_tbl %>% dplyr::filter(Significance == "Upregulated") %>%
    dplyr::arrange(dplyr::desc(abs(logFC)), Minus_Log10_FDR) %>% dplyr::slice_head(n = top_n),
  res_tbl %>% dplyr::filter(Significance == "Downregulated") %>%
    dplyr::arrange(dplyr::desc(abs(logFC)), Minus_Log10_FDR) %>% dplyr::slice_head(n = bottom_n)
) %>% dplyr::mutate(label = gene_symbol)

res_tbl <- res_tbl %>% dplyr::mutate(label = gene_symbol)

p_volcano <- ggplot(res_tbl,
  aes(x = logFC, y = Minus_Log10_FDR, color = Significance, label = label)) +
  geom_point(alpha = 0.8, size = 1.8) +
  scale_color_manual(values = c("Not Significant" = "grey70",
                                "Upregulated"     = "#d73027",
                                "Downregulated"   = "#4575b4")) +
  geom_vline(xintercept = c(-lfc_thr, lfc_thr), col = "black", linetype = "dashed") +
  geom_hline(yintercept = -log10(fdr_thr),      col = "black", linetype = "dashed") +
  ggrepel::geom_text_repel(data = label_df, size = 3, max.overlaps = Inf,
                           box.padding = 0.25, min.segment.length = 0) +
  theme_minimal() +
  labs(x = "Log2 (Fold Change)", y = "-Log10 (FDR)") +
  theme(plot.title   = element_text(hjust = 0.5, size = 18),
        legend.position = "none",
        axis.title   = element_text(size = 18),
        axis.text    = element_text(size = 16),
        panel.border = element_blank(),
        axis.line    = element_line()) +
  scale_x_continuous(breaks = seq(-1, 1, by = 1), limits = c(-1.2, 1.8))

ggsave(file.path(OUT_DIR, "Fig4a_volcano.pdf"), plot = p_volcano,
       width = 7, height = 6, device = "pdf")
readr::write_csv(res_tbl, file.path(OUT_DIR, "limma_results.csv"))

n_sig <- sum(res_tbl$adj.P.Val < fdr_thr & abs(res_tbl$logFC) >= lfc_thr, na.rm = TRUE)
cat(sprintf("[limma] DE proteins (FDR < %.2g & |logFC| >= %.2f) = %d\n", fdr_thr, lfc_thr, n_sig))


################################################################################
###### Part 3: GSEA Pathway Enrichment (Fig 4b) ------
################################################################################

### Build geneList from limma results
make_geneList <- function(res_tbl, score_cols = c("t", "stat", "logFC", "beta"),
                          id_col = "gene_symbol", use_orgdb = org.Hs.eg.db) {
  stopifnot(is.data.frame(res_tbl))
  sc <- score_cols[score_cols %in% names(res_tbl)]
  if (length(sc) == 0) stop("No usable score column in res_tbl")
  score_col <- sc[1]

  df <- res_tbl %>%
    dplyr::select(dplyr::any_of(c(id_col, score_col, "logFC", "adj.P.Val", "P.Value"))) %>%
    dplyr::mutate(
      stat_tmp = dplyr::case_when(
        !is.na(.data[[score_col]]) ~ as.numeric(.data[[score_col]]),
        "P.Value" %in% names(.) & !all(is.na(.data[["P.Value"]])) ~
          sign(.data[["logFC"]]) * (-log10(pmax(.data[["P.Value"]], 1e-300))),
        "adj.P.Val" %in% names(.) ~
          sign(.data[["logFC"]]) * (-log10(pmax(.data[["adj.P.Val"]], 1e-300))),
        TRUE ~ NA_real_
      )
    ) %>%
    dplyr::filter(!is.na(.data[[id_col]]), !is.na(.data[["stat_tmp"]])) %>%
    dplyr::distinct(.data[[id_col]], .keep_all = TRUE)

  sym2ent <- clusterProfiler::bitr(df[[id_col]], fromType = "SYMBOL",
                                   toType = "ENTREZID", OrgDb = use_orgdb)

  df2 <- df %>%
    dplyr::inner_join(sym2ent, by = setNames("SYMBOL", id_col)) %>%
    dplyr::select(ENTREZID, stat = stat_tmp) %>%
    dplyr::group_by(ENTREZID) %>%
    dplyr::slice_max(order_by = abs(stat), n = 1, with_ties = FALSE) %>%
    dplyr::ungroup() %>%
    dplyr::arrange(dplyr::desc(stat))

  geneList <- df2$stat
  names(geneList) <- df2$ENTREZID
  geneList
}

geneList <- make_geneList(res_tbl)
stopifnot(length(geneList) >= 100)

### GSEA: GO (BP / CC / MF / ALL)
ego_bp <- gseGO(geneList, OrgDb = org.Hs.eg.db, keyType = "ENTREZID",
                ont = "BP", minGSSize = 15, maxGSSize = 500,
                pAdjustMethod = "BH", pvalueCutoff = 0.1,
                verbose = FALSE, eps = 1e-10)
save(ego_bp, file = file.path(OUT_DIR, "GSEA_GO_BP.rdata"))

ego_cc <- gseGO(geneList, OrgDb = org.Hs.eg.db, keyType = "ENTREZID",
                ont = "CC", minGSSize = 15, maxGSSize = 500,
                pAdjustMethod = "BH", pvalueCutoff = 0.1,
                verbose = FALSE, eps = 1e-10)
save(ego_cc, file = file.path(OUT_DIR, "GSEA_GO_CC.rdata"))

ego_mf <- gseGO(geneList, OrgDb = org.Hs.eg.db, keyType = "ENTREZID",
                ont = "MF", minGSSize = 15, maxGSSize = 500,
                pAdjustMethod = "BH", pvalueCutoff = 0.1,
                verbose = FALSE, eps = 1e-10)
save(ego_mf, file = file.path(OUT_DIR, "GSEA_GO_MF.rdata"))

ego_all <- gseGO(geneList, OrgDb = org.Hs.eg.db, keyType = "ENTREZID",
                 ont = "ALL", minGSSize = 15, maxGSSize = 500,
                 pAdjustMethod = "BH", pvalueCutoff = 0.1,
                 verbose = FALSE, eps = 1e-10)
save(ego_all, file = file.path(OUT_DIR, "GSEA_GO_ALL.rdata"))

### GSEA: KEGG
ekegg <- try(
  gseKEGG(geneList, organism = "hsa", minGSSize = 10, maxGSSize = 500,
          pAdjustMethod = "BH", pvalueCutoff = 0.1,
          verbose = FALSE, eps = 1e-10),
  silent = TRUE
)
if (inherits(ekegg, "try-error")) {
  warning("gseKEGG failed (KEGG API issue); skipping KEGG step.")
  ekegg <- NULL
} else {
  save(ekegg, file = file.path(OUT_DIR, "GSEA_KEGG.rdata"))
}

### Pick top pathways
pick_top_ids <- function(gsea_obj, n_each = 20, padj_thr = 0.05) {
  df <- as.data.frame(gsea_obj@result)
  if (!all(c("ID", "NES", "p.adjust") %in% names(df))) return(character(0))
  df <- df %>% dplyr::filter(!is.na(NES))
  sig <- df %>% dplyr::filter(p.adjust < padj_thr)
  if (nrow(sig) == 0) sig <- df
  sig %>% dplyr::arrange(dplyr::desc(abs(NES))) %>%
    dplyr::pull(ID) %>% head(n_each)
}

ids_gobp  <- pick_top_ids(ego_bp, 10); ids_gocc <- pick_top_ids(ego_cc, 10)
ids_gomf  <- pick_top_ids(ego_mf, 10); ids_goall <- pick_top_ids(ego_all, 10)
ids_kegg  <- if (!is.null(ekegg)) pick_top_ids(ekegg, 10) else character(0)

### GSEA curve plots per pathway
plot_gsea_curves <- function(obj, ids, outdir, prefix) {
  dir.create(outdir, showWarnings = FALSE, recursive = TRUE)
  if (length(ids) == 0) return(invisible(NULL))
  for (id in ids) {
    clean_name <- gsub("[/:\\\\]", "-", id)
    fn <- file.path(outdir, sprintf("%s__%s.pdf", prefix, clean_name))
    p <- GseaVis::gseaNb(object = obj, geneSetID = id, addPval = TRUE, pvalY = 0.8)
    ggplot2::ggsave(fn, plot = p, width = 6.5, height = 5.5, device = "pdf")
  }
}

plot_gsea_curves(ego_bp, ids_gobp,  file.path(OUT_DIR, "GSEA_GO_BP"), "GO_BP")
plot_gsea_curves(ego_cc, ids_gocc,  file.path(OUT_DIR, "GSEA_GO_CC"), "GO_CC")
plot_gsea_curves(ego_mf, ids_gomf,  file.path(OUT_DIR, "GSEA_GO_MF"), "GO_MF")
plot_gsea_curves(ego_all, ids_goall, file.path(OUT_DIR, "GSEA_GO_ALL"), "GO_ALL")
if (length(ids_kegg) > 0) plot_gsea_curves(ekegg, ids_kegg, file.path(OUT_DIR, "GSEA_KEGG"), "KEGG")

### Dotplot overview
dotplot_top <- function(gsea_obj, n_show = 15) {
  enrichplot::dotplot(gsea_obj, showCategory = n_show) +
    ggplot2::labs(title = "Top pathways by FDR") +
    ggplot2::theme(axis.text.y = ggplot2::element_text(size = 8))
}

pdf(file.path(OUT_DIR, "Fig4b_GO_BP_dotplot.pdf"), width = 7, height = 5.5)
print(dotplot_top(ego_bp, 15)); dev.off()
if (length(ids_kegg) > 0) {
  pdf(file.path(OUT_DIR, "Fig4b_KEGG_dotplot.pdf"), width = 7, height = 5.5)
  print(dotplot_top(ekegg, 15)); dev.off()
}

### GSEA combined pathway plot (Fig 4b)
pal <- c(BP = '#51A39D', CC = '#709B2B', MF = "#C65146", KEGG = '#eaa052')

build_pathway_df <- function(ego_bp, ego_cc, ego_mf, ekegg, ids_bp, ids_cc, ids_mf, ids_kg, n_top = 10) {
  extract_rows <- function(obj, ids, ont) {
    if (length(ids) == 0) return(NULL)
    df <- as.data.frame(obj@result) %>%
      dplyr::filter(ID %in% ids) %>%
      dplyr::select(ID, Description, NES, p.adjust, setSize) %>%
      dplyr::rename(Count = setSize) %>%
      dplyr::mutate(ONTOLOGY = ont)
    df
  }
  bind_rows(
    extract_rows(ego_bp, ids_bp, "BP"),
    extract_rows(ego_cc, ids_cc, "CC"),
    extract_rows(ego_mf, ids_mf, "MF"),
    if (!is.null(ekegg)) extract_rows(ekegg, ids_kg, "KEGG") else NULL
  ) %>%
    dplyr::arrange(ONTOLOGY, dplyr::desc(abs(NES))) %>%
    dplyr::mutate(index = dplyr::row_number())
}

use_pathway <- build_pathway_df(ego_bp, ego_cc, ego_mf, ekegg,
                                 ids_gobp, ids_gocc, ids_gomf, ids_kegg)

if (nrow(use_pathway) > 0) {
  rect.data <- use_pathway %>%
    dplyr::group_by(ONTOLOGY) %>%
    dplyr::summarise(ymin = min(index) - 0.35, ymax = max(index) + 0.35, .groups = "drop")

  bar.data <- use_pathway %>%
    dplyr::mutate(xmin = 0, xmax = pmax(0, -log10(p.adjust)),
                  ymin = index - 0.30, ymax = index + 0.30)

  p_enrichment <- ggplot(use_pathway, aes(x = -log10(p.adjust), y = index, fill = ONTOLOGY)) +
    ggforce::geom_round_rect(
      aes(xmin = -0.36, xmax = -0.15, ymin = ymin, ymax = ymax, fill = ONTOLOGY),
      data = rect.data, inherit.aes = FALSE, alpha = 0.8, radius = grid::unit(1.8, "mm")
    ) +
    geom_text(data = rect.data,
              aes(x = -0.255, y = (ymin + ymax) / 2, label = ONTOLOGY),
              inherit.aes = FALSE, size = 4.5) +
    geom_point(aes(x = -0.07, y = index, size = Count, fill = ONTOLOGY),
               shape = 21, colour = "black", stroke = 0.5) +
    geom_text(aes(x = -0.07, y = index, label = Count), vjust = 0.6, size = 3.6) +
    ggforce::geom_round_rect(
      data = bar.data,
      aes(xmin = xmin, xmax = xmax, ymin = ymin, ymax = ymax, fill = ONTOLOGY),
      inherit.aes = FALSE, alpha = 0.85, radius = grid::unit(3, "pt")
    ) +
    geom_text(data = use_pathway,
              aes(x = 0.020, y = index, label = Description),
              inherit.aes = FALSE, hjust = 0, size = 4.6) +
    labs(y = NULL, x = expression(-log[10](p.adjust))) +
    scale_fill_manual(name = "Category", values = pal) +
    scale_size_continuous(name = "Count", range = c(5, 11)) +
    scale_x_continuous(limits = c(-0.38, 3), breaks = c(0, 1, 2, 3),
                       expand = expansion(c(0, 0))) +
    ggprism::theme_prism() +
    theme(
      axis.text.y   = element_blank(), axis.ticks.y = element_blank(),
      axis.line.y   = element_blank(),
      axis.line.x   = element_line(linewidth = 1.3, colour = "black"),
      legend.position = c(0.85, 0.60),
      legend.background = element_rect(fill = "transparent", colour = NA)
    ) +
    coord_cartesian(clip = "off")

  ggsave(file.path(OUT_DIR, "Fig4b_GSEA_combined.pdf"), p_enrichment,
         width = 10, height = 6, device = "pdf")
}

message("[GSEA] Enrichment analysis done")


################################################################################
###### Part 4: Elastic Net Feature Selection ------
################################################################################

dat_all <- score_tbl %>%
  dplyr::inner_join(dat_protein, by = "eid") %>%
  dplyr::inner_join(covars, by = "eid")

sc <- as.character(score_col)[1]
if (!is.na(sc) && nzchar(sc) && sc %in% names(dat_all)) {
  dat_all <- dat_all %>% dplyr::rename(overall_score = .data[[sc]])
}
SCORE_COL <- "overall_score"
stopifnot(SCORE_COL %in% names(dat_all))

protein_cols_all <- grep(protein_regex, names(dat_all), value = TRUE)
cand_from_volcano <- res_tbl %>%
  dplyr::filter(adj.P.Val <= UNI_FDR_THR) %>%
  dplyr::pull(protein_id) %>% unique()

candidates <- base::intersect(cand_from_volcano, protein_cols_all)
stopifnot(length(candidates) > 0)

missing_rate <- colMeans(is.na(dat_all[, candidates, drop = FALSE]))
keep_prot_en <- names(missing_rate[missing_rate <= miss_thr])
stopifnot(length(keep_prot_en) > 0)

dat_imp <- dat_all %>%
  dplyr::mutate(across(all_of(keep_prot_en),
                       ~ ifelse(is.na(.), median(., na.rm = TRUE), .)))

### Build X / y (covariates unpenalized)
y <- dat_imp[[SCORE_COL]]
X_prot <- as.matrix(dat_imp[, keep_prot_en, drop = FALSE])

COVARS <- COVARS_CANDIDATES[COVARS_CANDIDATES %in% names(dat_imp)]
if (length(COVARS)) {
  mm <- stats::model.matrix(~ . - 1, data = dat_imp[, COVARS, drop = FALSE])
  X_cov <- as.matrix(mm)
  X_full <- cbind(X_prot, X_cov)
  pen <- c(rep(1, ncol(X_prot)), rep(0, ncol(X_cov)))
  covar_names <- colnames(X_cov)
} else {
  X_full <- X_prot; pen <- rep(1, ncol(X_prot)); covar_names <- character(0)
}

### Alpha grid search
cv_list <- vector("list", length(ALPHA_GRID))
summ_en <- tibble()

for (i in seq_along(ALPHA_GRID)) {
  a <- ALPHA_GRID[i]
  cvfit <- glmnet::cv.glmnet(X_full, y, family = "gaussian", alpha = a,
                             nfolds = NFOLDS_EN, type.measure = "mse",
                             standardize = TRUE, penalty.factor = pen)
  summ_en <- bind_rows(summ_en,
                       tibble(alpha = a, lambda = cvfit$lambda.min,
                              cvm = min(cvfit$cvm, na.rm = TRUE)))
  cv_list[[i]] <- cvfit
}

summ_en  <- dplyr::as_tibble(summ_en)
best_row <- dplyr::arrange(summ_en, cvm) %>% dplyr::slice_head(n = 1)
alpha_star  <- best_row$alpha[[1]]
lambda_star <- best_row$lambda[[1]]
cvfit_star  <- cv_list[[which(ALPHA_GRID == alpha_star)]]

### Extract non-zero coefficients
coef_star <- as.matrix(coef(cvfit_star, s = lambda_star))
coef_tbl  <- tibble(Feature = rownames(coef_star),
                    Coef = as.numeric(coef_star[, 1])) %>%
  dplyr::filter(Feature != "(Intercept)") %>%
  dplyr::mutate(is_protein = Feature %in% colnames(X_prot),
                is_cov     = Feature %in% covar_names)

sel_prot <- coef_tbl %>%
  dplyr::filter(is_protein, Coef != 0) %>%
  dplyr::arrange(desc(abs(Coef)))

mm <- mapping %>%
  dplyr::select(protein_id, gene_symbol) %>%
  as.data.frame()
tmp <- sel_prot %>%
  dplyr::left_join(mm, by = c("Feature" = "protein_id")) %>%
  dplyr::mutate(Label = dplyr::coalesce(gene_symbol, Feature))
tmp$alpha  <- alpha_star; tmp$lambda <- lambda_star
enet_selected_proteins <- tmp

readr::write_csv(summ_en,                file.path(OUT_DIR, "enet_alpha_grid_summary.csv"))
readr::write_csv(enet_selected_proteins, file.path(OUT_DIR, "enet_selected_proteins.csv"))
readr::write_csv(coef_tbl,               file.path(OUT_DIR, "enet_coef_full.csv"))

cat(sprintf("[EN] alpha* = %.2f, lambda* = %.4g, selected proteins = %d\n",
            alpha_star, lambda_star, nrow(enet_selected_proteins)))
if (nrow(enet_selected_proteins)) {
  message("Top 10 by |Coef|:")
  print(enet_selected_proteins %>% dplyr::slice_head(n = 10))
}


################################################################################
###### Part 5: Protein-Disease C-index (Fig 4c) ------
################################################################################

sel_df <- enet_selected_proteins %>%
  dplyr::transmute(Protein = Feature,
                   Label   = dplyr::coalesce(Label, Feature),
                   Coefficient = Coef)

total <- read_any(disease_file)

pick_id <- function(df) {
  nm <- names(df)
  if ("eid" %in% nm) return("eid")
  if ("n_eid" %in% nm) return("n_eid")
  stop("Cannot find ID column (need eid or n_eid)")
}
ID_DATA <- pick_id(dat_imp); ID_DIS <- pick_id(total)

OUTCOME_PREFIX <- "death"
bl_col <- paste0(OUTCOME_PREFIX, "_baseline")
ti_col <- paste0(OUTCOME_PREFIX, "_time")
ev_col <- paste0(OUTCOME_PREFIX, "_incident")
need   <- c(ID_DIS, bl_col, ti_col, ev_col)
stopifnot(all(need %in% names(total)))

rhs <- total %>% tibble::as_tibble() %>%
  dplyr::select(dplyr::all_of(need)) %>%
  dplyr::rename(!!rlang::sym(ID_DATA) := dplyr::all_of(ID_DIS),
                baseline = dplyr::all_of(bl_col),
                time     = dplyr::all_of(ti_col),
                event    = dplyr::all_of(ev_col))

dat_surv <- dat_imp %>%
  dplyr::left_join(rhs, by = ID_DATA) %>%
  dplyr::mutate(time = as.numeric(time), event = as.integer(event),
                baseline = as.integer(baseline))

dat0 <- dat_surv %>%
  dplyr::filter(baseline == 0) %>%
  dplyr::filter(!is.na(time), !is.na(event))

### Protein filtering
prot_in_data <- intersect(sel_df$Protein, names(dat0))
is_numeric   <- vapply(dat0[prot_in_data], is.numeric, logical(1))
prot_in_data <- prot_in_data[is_numeric]
nzv_mask <- vapply(dat0[prot_in_data],
                   function(v) stats::var(v, na.rm = TRUE) > 0, logical(1))
prot_in_data <- prot_in_data[nzv_mask]

sel_df <- sel_df %>%
  dplyr::filter(Protein %in% prot_in_data) %>%
  dplyr::mutate(coef_abs = abs(Coefficient)) %>%
  dplyr::arrange(desc(coef_abs), Protein)

message(sprintf("[C-index] Proteins for CV: %d; N: %d", nrow(sel_df), nrow(dat0)))

### C-index cross-validation (following MainAnalysis.R)
folds <- createFolds(as.factor(dat0$event), k = KFOLDS, list = TRUE, returnTrain = FALSE)

cv_cox_cindex <- function(df, predictors, folds) {
  if (!length(predictors)) return(tibble(c_index = NA_real_, lo = NA_real_,
                                         hi = NA_real_, folds = 0))
  cvals <- numeric(length(folds))
  need_cols <- c("time", "event", predictors)
  for (i in seq_along(folds)) {
    te <- df[folds[[i]], , drop = FALSE]
    tr <- df[-folds[[i]], , drop = FALSE]
    tr <- tr[complete.cases(tr[, need_cols, drop = FALSE]), , drop = FALSE]
    te <- te[complete.cases(te[, need_cols, drop = FALSE]), , drop = FALSE]
    if (nrow(tr) < 3 || sum(tr$event == 1) == 0 ||
        nrow(te) < 2 || sum(te$event == 1) == 0) { cvals[i] <- NA_real_; next }

    form <- as.formula(paste0("Surv(time, event) ~ ", paste(predictors, collapse = " + ")))
    fit <- tryCatch(coxph(form, data = tr, ties = "efron"), error = function(e) NULL)
    if (is.null(fit)) { cvals[i] <- NA_real_; next }

    pr <- tryCatch(predict(fit, newdata = te, type = "risk"), error = function(e) NA)
    ok <- is.finite(pr)
    if (!any(ok)) { cvals[i] <- NA_real_; next }

    ci <- survcomp::concordance.index(x = pr[ok], surv.time = te$time[ok],
                                      surv.event = te$event[ok])
    cvals[i] <- ci$c.index
  }
  cvals <- cvals[is.finite(cvals)]
  if (!length(cvals)) return(tibble(c_index = NA_real_, lo = NA_real_, hi = NA_real_, folds = 0))
  mean_ci <- mean(cvals); se_ci <- sd(cvals) / sqrt(length(cvals))
  tibble(c_index = round(mean_ci, 4),
         lo = round(mean_ci - 1.96 * se_ci, 4),
         hi = round(mean_ci + 1.96 * se_ci, 4),
         folds = length(cvals))
}

### Full-model C-index
res_full <- cv_cox_cindex(dat0, sel_df$Protein, folds)
res_full$vars <- nrow(sel_df)
readr::write_csv(res_full, file.path(OUT_DIR, paste0("cindex_full_", OUTCOME_PREFIX, ".csv")))

### Single-protein C-index
plan(multisession); handlers(global = TRUE)
with_progress({
  p <- progressor(steps = nrow(sel_df))
  single_rows <- future_lapply(seq_len(nrow(sel_df)), function(i) {
    p(); marker <- sel_df$Protein[i]
    x <- cv_cox_cindex(dat0, marker, folds)
    tibble(Protein = marker, c_index = x$c_index, lo = x$lo, hi = x$hi, folds = x$folds)
  }, future.seed = TRUE)
})
plan(sequential)

single_df <- dplyr::bind_rows(single_rows) %>%
  dplyr::left_join(sel_df %>% dplyr::select(Protein, Label, Coefficient), by = "Protein") %>%
  dplyr::mutate(Label = dplyr::coalesce(Label, Protein), coef_abs = abs(Coefficient))

single_ranked <- single_df %>%
  dplyr::filter(is.finite(c_index)) %>%
  dplyr::arrange(dplyr::desc(c_index))
readr::write_csv(single_ranked,
                 file.path(OUT_DIR, paste0("cindex_single_", OUTCOME_PREFIX, ".csv")))

### Top-N cumulative C-index
topN <- min(40, nrow(single_ranked))
topN_vec <- single_ranked$Protein[1:topN]

plan(multisession)
with_progress({
  p <- progressor(steps = topN)
  curve_rows <- future_lapply(seq_len(topN), function(N) {
    p(); x <- cv_cox_cindex(dat0, topN_vec[1:N], folds)
    tibble(N = N, c_index = x$c_index, lo = x$lo, hi = x$hi)
  }, future.seed = TRUE)
})
plan(sequential)
curve_df <- dplyr::bind_rows(curve_rows)
readr::write_csv(curve_df, file.path(OUT_DIR, paste0("cindex_topN_curve_", OUTCOME_PREFIX, ".csv")))

### C-index visualization (Fig 4c)
p_cindex <- ggplot(curve_df, aes(x = N, y = c_index)) +
  geom_ribbon(aes(ymin = lo, ymax = hi), alpha = 0.20) +
  geom_smooth(method = "loess", se = FALSE, span = 0.6, linewidth = 1.2) +
  geom_point(size = 1.8, alpha = 0.85) +
  geom_hline(yintercept = 0.5, linetype = "dashed", linewidth = 0.4) +
  scale_x_continuous(breaks = seq(0, topN, by = 5)) +
  labs(x = "Number of proteins", y = "C-index (95% CI)") +
  theme_minimal(base_family = "Arial") +
  theme(panel.grid.major = element_line(color = "grey85", linetype = "dotted"),
        panel.grid.minor = element_blank(),
        axis.title = element_text(size = 12), axis.text = element_text(size = 10))

ggsave(file.path(OUT_DIR, paste0("Fig4c_cindex_curve_", OUTCOME_PREFIX, ".pdf")),
       p_cindex, width = 6.5, height = 5, device = "pdf")

### Single-protein forest plot (Fig 4c supplement)
single_plot_df <- single_ranked %>%
  dplyr::mutate(rank = dplyr::row_number(),
                color_grp = case_when(c_index > 0.6 ~ "High (>0.6)",
                                      lo >= 0.5   ~ "Medium (lo>=0.5)",
                                      TRUE         ~ "Low (lo<0.5)"))
cols <- c("Low (lo<0.5)" = "#45a8bb", "Medium (lo>=0.5)" = "#eed196", "High (>0.6)" = "#c8561f")

p_single <- ggplot(single_plot_df, aes(x = rank, y = c_index, colour = color_grp)) +
  geom_errorbar(aes(ymin = lo, ymax = hi), width = 0.2, alpha = 0.8) +
  geom_point(size = 1.6) +
  geom_hline(yintercept = 0.5, linetype = "dashed", alpha = 0.5) +
  scale_color_manual(values = cols) +
  labs(x = "Protein rank", y = "C-index") +
  theme_bw(base_size = 14) + theme(legend.position = c(0.85, 0.25))

ggsave(file.path(OUT_DIR, paste0("Fig4c_single_cindex_", OUTCOME_PREFIX, ".pdf")),
       p_single, width = 10, height = 5, dpi = 300)

message("[C-index] Protein-disease C-index analysis done")

################################################################################
###### Part 6: Mediation Analysis (Fig 4d) ------
################################################################################

### Cox screen (protein × disease, adjusted for covariates)
cand_vec <- unique(enet_selected_proteins$Feature)
cand_vec <- cand_vec[cand_vec %in% names(dat0)]

cox_screen_one <- function(prot) {
  rhs <- c(prot, SCORE_COL, COVARS)
  fml <- as.formula(paste0("survival::Surv(time, event) ~ ", paste(rhs, collapse = " + ")))
  df <- dat0[, c("time", "event", rhs), drop = FALSE]
  df <- df[complete.cases(df), , drop = FALSE]
  if (nrow(df) < 50 || sum(df$event) < 5)
    return(tibble(Protein = prot, beta = NA_real_, se = NA_real_, p = NA_real_, n = nrow(df)))
  fit <- tryCatch(coxph(fml, data = df, ties = "efron"), error = function(e) NULL)
  if (is.null(fit)) return(tibble(Protein = prot, beta = NA_real_, se = NA_real_,
                                  p = NA_real_, n = nrow(df)))
  tt <- tryCatch(broom::tidy(fit), error = function(e) NULL)
  if (is.null(tt)) return(tibble(Protein = prot, beta = NA_real_, se = NA_real_,
                                 p = NA_real_, n = nrow(df)))
  row <- tt[tt$term == prot, , drop = FALSE]
  if (!nrow(row)) return(tibble(Protein = prot, beta = NA_real_, se = NA_real_,
                                p = NA_real_, n = nrow(df)))
  tibble(Protein = prot, beta = row$estimate, se = row$std.error,
         p = row$p.value, n = nrow(df))
}

plan(multisession)
with_progress({
  p <- progressor(steps = length(cand_vec))
  cox_list <- future_lapply(cand_vec, function(pr) { p(); cox_screen_one(pr) },
                            future.seed = TRUE)
})
plan(sequential)

cox_scr <- dplyr::bind_rows(cox_list) %>%
  dplyr::mutate(
    p = suppressWarnings(as.numeric(p)),
    p = ifelse(is.na(p) | !is.finite(p) | p <= 0, NA_real_, p),
    p_adj  = p.adjust(p, method = "BH"),
    HR = exp(beta), HR_LCL = exp(beta - 1.96 * se), HR_UCL = exp(beta + 1.96 * se),
    minus_log10_fdr = ifelse(is.na(p_adj) | p_adj <= 0, NA_real_, -log10(p_adj))
  ) %>%
  dplyr::left_join(mapping %>% dplyr::select(protein_id, gene_symbol),
                   by = c("Protein" = "protein_id")) %>%
  dplyr::mutate(Label = dplyr::coalesce(gene_symbol, Protein))

readr::write_csv(
  cox_scr %>% dplyr::select(Protein, Label, n, beta, se, HR, HR_LCL, HR_UCL,
                            p, p_adj, minus_log10_fdr),
  file.path(OUT_DIR, paste0("cox_screen_", OUTCOME_PREFIX, ".csv"))
)

sig_cox <- cox_scr %>%
  dplyr::filter(is.finite(p_adj), p_adj < SIG_FDR_THR) %>%
  dplyr::arrange(p_adj)
sig_vec <- sig_cox$Protein

message(sprintf("[Mediation-screen] Candidates = %d, Significant (FDR < %.2f) = %d",
                length(cand_vec), SIG_FDR_THR, length(sig_vec)))

### Weibull survival mediation
if (length(sig_vec) == 0) {
  warning("No significant proteins; skipping mediation analysis.")
} else {
  dat_med <- dat0 %>%
    dplyr::select(eid, time, event, all_of(SCORE_COL), all_of(COVARS), all_of(sig_vec)) %>%
    dplyr::mutate(across(all_of(COVARS), as.numeric))

  run_one_med_surv <- function(protein) {
    dat <- dat_med %>% dplyr::rename(M = all_of(protein))
    modM <- lm(reformulate(c(SCORE_COL, COVARS), "M"), data = dat)
    modY <- survival::survreg(
      as.formula(paste0("Surv(time, event) ~ ", SCORE_COL, " + M + ",
                        paste(COVARS, collapse = " + "))),
      data = dat, dist = "weibull")
    med <- mediate(modM, modY, treat = SCORE_COL, mediator = "M",
                   sims = 500, boot.ci.type = "perc")

    gene_lab <- mapping$gene_symbol[mapping$protein_id == protein][1]
    tibble(
      Protein = protein, Label = dplyr::coalesce(gene_lab, protein),
      ACME = med$d1[1], ACME_lower = med$d1.ci[1], ACME_upper = med$d1.ci[2],
      ADE  = med$z1[1], ADE_lower  = med$z1.ci[1], ADE_upper  = med$z1.ci[2],
      PropMed = med$n1[1], PropMed_lower = med$n1.ci[1], PropMed_upper = med$n1.ci[2],
      p_ACME = med$p.value.d1, p_ADE = med$p.value.z1, n_sample = nrow(dat)
    )
  }

  plan(multisession, workers = max(1, parallel::detectCores() - 1))
  med_list_surv <- future_lapply(sig_vec, run_one_med_surv, future.seed = TRUE)
  plan(sequential)

  med_df <- dplyr::bind_rows(med_list_surv) %>% dplyr::filter(!is.na(ACME))
  readr::write_csv(med_df,
                   file.path(OUT_DIR, paste0("mediation_weibull_", OUTCOME_PREFIX, ".csv")))
  cat(sprintf("[Mediation-survival] Done, %d proteins\n", nrow(med_df)))
}


################################################################################
###### Part 7: Mediation Bubble Plot (Fig 4d) ------
################################################################################

### Bubble: x = HR, y = ACME, size = Proportion Mediated
plot_mediation_bubble <- function(med_df, cox_scr, top_n = 25, out_dir = OUT_DIR) {
  if (!exists("med_df") || nrow(med_df) == 0) return(invisible(NULL))
  message("===== Mediation Bubble (Fig 4d) =====")

  plot_data <- med_df %>%
    dplyr::inner_join(
      cox_scr %>% dplyr::select(Protein, HR, HR_LCL, HR_UCL),
      by = "Protein"
    ) %>%
    dplyr::arrange(desc(abs(PropMed))) %>%
    dplyr::slice_head(n = top_n) %>%
    dplyr::arrange(desc(HR)) %>%
    dplyr::mutate(
      y_pos    = dplyr::row_number(),
      signACME = factor(sign(ACME), levels = c(-1, 1),
                        labels = c("Negative", "Positive")),
      absPM    = abs(PropMed)
    )

  p <- ggplot(plot_data, aes(x = HR, y = y_pos)) +
    geom_vline(xintercept = 1, linetype = "dashed", colour = "grey60", linewidth = 1) +
    geom_segment(aes(xend = 1, yend = y_pos, colour = signACME),
                 linewidth = 1.5, alpha = 0.6) +
    geom_point(aes(size = absPM, fill = signACME),
               shape = 21, colour = "white", stroke = 0.5) +
    scale_colour_manual(values = c(Negative = "#2E6FBB", Positive = "#D64B3B")) +
    scale_fill_manual(values = c(Negative = "#2E6FBB", Positive = "#D64B3B")) +
    scale_size_continuous(name = "|Proportion Mediated|", range = c(3, 10),
                          breaks = c(0.15, 0.20, 0.25)) +
    scale_y_continuous(breaks = plot_data$y_pos, labels = plot_data$Label) +
    labs(x = "Hazard Ratio (HR)", y = NULL,
         title = "Mediation of LLM dietary score -> all-cause mortality") +
    theme_minimal(base_family = "Arial") +
    theme(panel.border = element_rect(fill = NA, colour = "black", linewidth = 0.6),
          panel.grid.major.y = element_blank(),
          legend.position = "right",
          plot.title = element_text(hjust = 0.5, size = 14),
          axis.text = element_text(size = 10))

  ggsave(file.path(out_dir, "Fig4d_mediation_bubble.pdf"),
         p, width = 7, height = 7, device = "pdf")
  message("Fig 4d saved")
  p
}

if (exists("med_df") && exists("cox_scr")) {
  plot_mediation_bubble(med_df, cox_scr)
}


################################################################################
###### Part 8: Protein Overlap + Novel Enrichment (Fig 4e-f) ------
################################################################################

### Fig 4e: UpSet plot — LLM-related proteins vs established diet index proteins
plot_protein_overlap <- function(
  protein_lists_file = file.path(OUT_DIR, "protein_overlap_lists.rds"),
  out_path = file.path(OUT_DIR, "Fig4e_protein_overlap.pdf")
) {
  message("===== Protein Overlap (Fig 4e) =====")

  if (file.exists(protein_lists_file)) {
    protein_lists <- readRDS(protein_lists_file)
  } else {
    message("  Protein list file not found: ", protein_lists_file)
    message("  Run differential expression for each dietary index first, then save the rds.")
    return(invisible(NULL))
  }

  if (!requireNamespace("UpSetR", quietly = TRUE)) {
    stop("Please install UpSetR: install.packages('UpSetR')")
  }

  all_prots <- unique(unlist(protein_lists))
  mat <- as.data.frame(sapply(protein_lists,
                              function(pl) as.integer(all_prots %in% pl)))
  colnames(mat) <- names(protein_lists)

  pdf(out_path, width = 8, height = 5)
  upset(mat, nsets = length(protein_lists),
        order.by = "freq", nintersects = 20,
        main.bar.color = "#2E9FDF", sets.bar.color = "#FC4E07")
  dev.off()
  message("Fig 4e saved")

  ### Count LLM-unique proteins
  llm_col <- grep("LLM", colnames(mat), value = TRUE, ignore.case = TRUE)[1]
  other_cols <- setdiff(colnames(mat), llm_col)
  llm_only <- all_prots[mat[[llm_col]] == 1 & rowSums(mat[, other_cols, drop = FALSE]) == 0]
  message(sprintf("  LLM-unique proteins: %d / total: %d", length(llm_only), length(all_prots)))
  invisible(llm_only)
}

### Fig 4f: GO enrichment of LLM-unique proteins
run_novel_enrichment <- function(llm_only_proteins, out_dir = OUT_DIR) {
  message("===== Novel Enrichment (Fig 4f) =====")

  if (length(llm_only_proteins) < 10) {
    warning("LLM-unique proteins < 10; skipping novel enrichment")
    return(invisible(NULL))
  }

  ego_novel <- enrichGO(
    gene          = llm_only_proteins,
    OrgDb         = org.Hs.eg.db,
    keyType       = "SYMBOL",
    ont           = "BP",
    pAdjustMethod = "BH",
    pvalueCutoff  = 0.05,
    qvalueCutoff  = 0.2
  )

  if (!is.null(ego_novel) && nrow(ego_novel@result) > 0) {
    p <- enrichplot::dotplot(ego_novel, showCategory = 15) +
      ggplot2::labs(title = "LLM-specific proteins: GO BP enrichment") +
      ggplot2::theme(axis.text.y = ggplot2::element_text(size = 9))
    ggsave(file.path(out_dir, "Fig4f_novel_enrichment.pdf"),
           p, width = 9, height = 6, device = "pdf")
    readr::write_csv(as.data.frame(ego_novel@result),
                     file.path(out_dir, "novel_enrichment_GO_BP.csv"))
    message("Fig 4f saved")
  }
  ego_novel
}

### Execute overlap + novel enrichment
llm_only <- plot_protein_overlap()
if (!is.null(llm_only)) run_novel_enrichment(llm_only)

message("===== 02_Proteomics_Analysis.R done =====")
message("Output directory: ", OUT_DIR)
