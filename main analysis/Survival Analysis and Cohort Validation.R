################################################################################
# 01_Survival_Analysis.R
# ============================================================================
# LLM dietary score vs 7 established indices: Cox survival + cross-cohort validation
# Paper: Figure 2a-c, Figure 3a-c
# Cohorts: UKB (206,416) / NHANES (34,696) / CHNS (17,450) /
#          HRS (4,222) / CLHLS (11,950) / XMC (30,882)
################################################################################

library(data.table)
library(readxl)
library(survival)
library(survminer)
library(survcomp)
library(caret)
library(future.apply)
library(dplyr)
library(tidyr)
library(tibble)
library(ggplot2)
library(readr)
library(purrr)

###### Global Parameters ------
set.seed(2024)
MIN_GROUP   <- 50
TIES_METHOD <- "breslow"
N_WORKERS   <- max(1, parallel::detectCores() - 1)
KFOLDS       <- 10

DATA_ROOT    <- "your/data/root/path"
UKB_ROOT     <- file.path(DATA_ROOT, "UKB")
NHANES_ROOT  <- file.path(DATA_ROOT, "NHANES")
CHNS_ROOT    <- file.path(DATA_ROOT, "CHNS")
HRS_ROOT     <- file.path(DATA_ROOT, "HRS")
CLHLS_ROOT   <- file.path(DATA_ROOT, "CLHLS")
XMC_ROOT     <- file.path(DATA_ROOT, "XMC")
OUT_ROOT     <- file.path(DATA_ROOT, "results")

dir.create(OUT_ROOT, showWarnings = FALSE, recursive = TRUE)

###### Utility Functions ------

### Scale to 0-100 by theoretical range; fallback to sample min-max
to100_by_range <- function(x, lo, hi) {
  if (all(is.na(x))) return(rep(NA_real_, length(x)))
  rng <- hi - lo
  if (!is.finite(rng) || rng <= 0) {
    xr <- range(x, na.rm = TRUE)
    if (!is.finite(xr[2] - xr[1]) || xr[2] - xr[1] <= 0) return(rep(50, length(x)))
    return((x - xr[1]) / (xr[2] - xr[1]) * 100)
  }
  (x - lo) / rng * 100
}

### Three-group split: low (bottom 10%), mid (45-55%), high (top 10%)
three_groups_10_10_10 <- function(x, jitter = TRUE) {
  v <- x
  if (jitter) { set.seed(2025); v <- v + runif(length(v), -1e-8, 1e-8) }
  ix <- which(!is.na(v)); n <- length(ix)
  grp <- rep(NA_character_, length(v))
  if (n == 0) return(grp)
  r <- rank(v[ix], ties.method = "average"); p <- r / n
  grp[ix[p <= 0.10]] <- "low"
  grp[ix[p >= 0.90]] <- "high"
  mid_idx <- ix[p >= 0.45 & p <= 0.55]; grp[mid_idx] <- "mid"
  grp
}

### Detect endpoints (matching _time + _incident pairs)
detect_endpoints <- function(cols) {
  times <- sub("_time$", "", cols[grepl("_time$", cols)])
  incs  <- sub("_incident$", "", cols[grepl("_incident$", cols)])
  pref  <- sort(intersect(times, incs))
  data.table(prefix = pref,
             has_base = paste0(pref, "_baseline") %in% cols)
}

### Format HR text as "HR (LCI-UCI)"
fmt_hr <- function(hr, lo, hi) {
  if (any(is.na(c(hr, lo, hi)))) return("NA")
  sprintf("%.2f (%.2f-%.2f)", hr, lo, hi)
}

### Safe CSV write with timestamp fallback if file is locked
safe_write_csv <- function(dt, path) {
  dir.create(dirname(path), showWarnings = FALSE, recursive = TRUE)
  tmp <- tempfile(fileext = ".csv")
  data.table::fwrite(dt, tmp)
  ok <- suppressWarnings(file.rename(tmp, path))
  if (!ok) {
    alt <- file.path(dirname(path),
                     sprintf("%s_%s.csv",
                             tools::file_path_sans_ext(basename(path)),
                             format(Sys.time(), "%Y%m%d-%H%M%S")))
    data.table::fwrite(dt, alt)
    message("File locked; written to: ", alt)
  } else {
    message("Written: ", path)
  }
}

### Read CSV or XLSX
read_any <- function(path) {
  ext <- tolower(tools::file_ext(path))
  if (ext %in% c("xlsx", "xls")) {
    readxl::read_excel(path)
  } else {
    suppressWarnings(readr::read_csv(path, show_col_types = FALSE, guess_max = 1e5))
  }
}

### Check if score column is E-DII (higher raw score = worse / pro-inflammatory)
is_EDII <- function(scol) grepl("(^|[^A-Za-z])E[._-]?DII", scol, ignore.case = TRUE)

### Parse HR text "HR (LCI-UCI)" to numeric components
parse_hr_row <- function(x) {
  if (is.na(x) || x == "NA") return(c(hr = NA_real_, lcl = NA_real_, ucl = NA_real_, sig = FALSE))
  sig <- grepl("\\*$", x)
  y   <- sub("\\*$", "", x)
  m   <- regexec("^\\s*([0-9.]+)\\s*\\(([0-9.]+)[–-]([0-9.]+)\\)\\s*$", y)
  z   <- regmatches(y, m)[[1]]
  if (length(z) == 4) {
    c(hr = as.numeric(z[2]), lcl = as.numeric(z[3]), ucl = as.numeric(z[4]), sig = sig)
  } else {
    c(hr = NA_real_, lcl = NA_real_, ucl = NA_real_, sig = sig)
  }
}

### 10-fold CV C-index (following MainAnalysis.R style)
cv_cindex <- function(df, time_col, event_col, predictor_cols, k = KFOLDS) {
  predictors <- predictor_cols[predictor_cols %in% names(df)]
  if (!length(predictors)) return(c(c_index = NA_real_, lo = NA_real_, hi = NA_real_))

  folds <- createFolds(as.factor(df[[event_col]]), k = k, list = TRUE)
  cvals <- numeric(length(folds))

  for (j in seq_along(folds)) {
    test_idx  <- folds[[j]]
    train_dat <- df[-test_idx, , drop = FALSE]
    test_dat  <- df[test_idx, , drop = FALSE]

    train_dat <- train_dat[complete.cases(train_dat[, c(time_col, event_col, predictors)]), ]
    test_dat  <- test_dat[complete.cases(test_dat[, c(time_col, event_col, predictors)]), ]
    if (nrow(train_dat) < 5 || nrow(test_dat) < 2) { cvals[j] <- NA_real_; next }

    fml <- as.formula(paste0("Surv(", time_col, ", ", event_col, ") ~ ",
                             paste(predictors, collapse = " + ")))
    fit <- tryCatch(coxph(fml, data = train_dat, ties = "efron"),
                    error = function(e) NULL)
    if (is.null(fit)) { cvals[j] <- NA_real_; next }

    pred_risk <- tryCatch(predict(fit, newdata = test_dat, type = "risk"),
                          error = function(e) NA)
    ok <- is.finite(pred_risk)
    if (!any(ok)) { cvals[j] <- NA_real_; next }

    ci <- survcomp::concordance.index(x = pred_risk[ok],
                                      surv.time = test_dat[[time_col]][ok],
                                      surv.event = test_dat[[event_col]][ok])
    cvals[j] <- ci$c.index
  }
  cvals <- cvals[is.finite(cvals)]
  if (!length(cvals)) return(c(c_index = NA_real_, lo = NA_real_, hi = NA_real_))
  mean_ci <- mean(cvals)
  se_ci   <- sd(cvals) / sqrt(length(cvals))
  c(c_index = round(mean_ci, 4),
    lo      = round(mean_ci - 1.96 * se_ci, 4),
    hi      = round(mean_ci + 1.96 * se_ci, 4))
}


################################################################################
###### UKB Multi-Score Cox Survival Analysis (Fig 2a, 2b, 2c) ------
################################################################################

### Theoretical ranges for 0-100 scaling
theo_ranges <- list(
  amed  = c(0, 9),
  hpdi  = c(18, 90),
  hei   = c(0, 100),
  mind  = c(0, 15),
  phdi  = c(0, 140),
  a_hei = c(0, 110)
)

### 0-100 scaling: theoretical ranges + E-DII reversal + LLM sample min-max
scale_to_100 <- function(dt, score_cols) {
  out <- vector("list", length(score_cols))
  names(out) <- paste0(score_cols, "_100")
  for (col in score_cols) {
    nm <- tolower(col); x <- dt[[col]]
    if (all(is.na(x))) { out[[paste0(col, "_100")]] <- NA_real_; next }

    if (grepl("dii", nm)) {
      tmp <- -x
      xr <- range(tmp, na.rm = TRUE)
      if (!is.finite(xr[2] - xr[1]) || xr[2] - xr[1] <= 0) {
        out[[paste0(col, "_100")]] <- rep(50, length(x))
      } else {
        out[[paste0(col, "_100")]] <- (tmp - xr[1]) / (xr[2] - xr[1]) * 100
      }
    } else {
      key <- names(theo_ranges)[vapply(names(theo_ranges), function(k) grepl(k, nm), logical(1))]
      if (length(key) > 0) {
        r <- theo_ranges[[key[1]]]
        out[[paste0(col, "_100")]] <- to100_by_range(x, r[1], r[2])
      } else {
        xr <- range(x, na.rm = TRUE)
        if (!is.finite(xr[2] - xr[1]) || xr[2] - xr[1] <= 0) {
          out[[paste0(col, "_100")]] <- rep(50, length(x))
        } else {
          out[[paste0(col, "_100")]] <- (x - xr[1]) / (xr[2] - xr[1]) * 100
        }
      }
    }
  }
  as.data.table(out)
}

### Read UKB multi-score data (7 established indices + LLM scores)
ukb_read_scores <- function() {
  score_root   <- file.path(UKB_ROOT, "scores")
  folders      <- c("AMED_score", "E_DII_score", "hpdi_score", "MIND_score",
                    "PHDI_score", "A-HEI_score", "HEI-2020_score")
  llm_score_xl <- file.path(score_root, "LLM_score", "UKB_LLM_score.xlsx")

  score_list <- list()
  for (fd in folders) {
    pdir <- file.path(score_root, fd)
    if (!dir.exists(pdir)) next
    fs <- list.files(pdir, pattern = "\\.(csv|xlsx)$", ignore.case = TRUE, full.names = TRUE)
    for (fp in fs) {
      dt <- tryCatch({
        if (grepl("\\.xlsx$", fp, ignore.case = TRUE)) as.data.table(readxl::read_xlsx(fp))
        else fread(fp)
      }, error = function(e) NULL)
      if (is.null(dt) || ncol(dt) < 2) next
      setnames(dt, 1, "eid"); dt[, eid := as.character(eid)]
      score_name <- paste0(basename(pdir), "_score")
      setnames(dt, names(dt)[2], score_name)
      score_list[[length(score_list) + 1]] <- dt[, .(eid, get(score_name))]
      setnames(score_list[[length(score_list)]], c("eid", score_name))
    }
  }

  big <- as.data.table(readxl::read_xlsx(llm_score_xl))
  setnames(big, names(big)[1], "eid")
  setnames(big, names(big), gsub("\\s+", "_", names(big)))
  big[, eid := as.character(eid)]
  for (j in names(big)[-1]) {
    tmp <- big[, .(eid, value = get(j))]
    setnames(tmp, c("eid", j))
    score_list[[length(score_list) + 1]] <- tmp
  }

  score_list <- Filter(function(x) !is.null(x) && nrow(x) > 0, score_list)
  stopifnot(length(score_list) > 0)
  Reduce(function(x, y) merge(x, y, by = "eid", all = TRUE), score_list)
}

### UKB Cox main pipeline
run_ukb_cox <- function() {
  message("===== UKB Cox Survival Analysis =====")
  score_dt <- ukb_read_scores()

  cov_csv <- file.path(UKB_ROOT, "covariates_mice_imputed.csv")
  dis_csv <- file.path(UKB_ROOT, "totaldisease.csv")

  cov_dt <- fread(cov_csv)
  if ("n_eid" %in% names(cov_dt) && !"eid" %in% names(cov_dt)) setnames(cov_dt, "n_eid", "eid")
  cov_dt[, eid := as.character(eid)]

  dz_dt <- fread(dis_csv)
  setnames(dz_dt, names(dz_dt), gsub("\\s+", "_", names(dz_dt)))
  if (!"eid" %in% names(dz_dt)) setnames(dz_dt, names(dz_dt)[1], "eid")
  dz_dt[, eid := as.character(eid)]

  dt <- score_dt %>%
    dplyr::inner_join(cov_dt, by = "eid") %>%
    dplyr::inner_join(dz_dt, by = "eid")
  setDT(dt)
  setkey(dt, eid)
  cat(sprintf("UKB data merged: N = %s\n", nrow(dt)))

  covars <- setdiff(names(cov_dt), "eid")
  for (c in covars) dt[[c]] <- suppressWarnings(as.numeric(dt[[c]]))

  ### 0-100 normalize + group
  score_cols <- grep("_score$", names(score_dt), value = TRUE)
  scaled <- scale_to_100(dt, score_cols)
  dt <- cbind(dt, scaled)

  score_100_cols <- names(scaled)
  grp_cols <- paste0(score_100_cols, "_grp")
  grp_dt <- dt[, lapply(.SD, function(z) three_groups_10_10_10(z, jitter = TRUE)),
               .SDcols = score_100_cols]
  setnames(grp_dt, names(grp_dt), grp_cols)
  dt <- cbind(dt, grp_dt)

  eps <- detect_endpoints(names(dz_dt))

  ### Single-task Cox: one score × one endpoint → mid/high rows
  run_one <- function(score_100_col, ep_row) {
    pfx      <- ep_row[["prefix"]]
    has_base <- isTRUE(ep_row[["has_base"]])
    grp_col  <- paste0(score_100_col, "_grp")
    t_col    <- paste0(pfx, "_time")
    e_col    <- paste0(pfx, "_incident")
    b_col    <- paste0(pfx, "_baseline")

    rowNA <- function(term) {
      data.frame(exposure = sub("_100$", "", score_100_col),
                 term = term, outcome = pfx,
                 total = NA_integer_, case = NA_integer_,
                 HR = NA_real_, LCI = NA_real_, UCI = NA_real_, P = NA_real_,
                 HR_text = NA_character_, stringsAsFactors = FALSE)
    }

    need <- c(grp_col, t_col, e_col)
    if (!all(need %in% names(dt))) return(rbind(rowNA("mid"), rowNA("high")))

    sub <- dt[, c(grp_col, t_col, e_col, covars), with = FALSE]
    if (has_base && b_col %in% names(dt)) sub <- sub[dt[[b_col]] == 0]

    sub[[t_col]] <- suppressWarnings(as.numeric(sub[[t_col]]))
    sub[[e_col]] <- as.integer(sub[[e_col]] > 0)
    sub <- sub[!is.na(sub[[grp_col]]) & !is.na(sub[[t_col]]) & !is.na(sub[[e_col]])]
    if (nrow(sub) == 0) return(rbind(rowNA("mid"), rowNA("high")))

    tab <- table(sub[[grp_col]])
    if (!all(c("low", "mid", "high") %in% names(tab)))
      return(rbind(rowNA("mid"), rowNA("high")))
    if (min(tab[c("low", "mid", "high")]) < MIN_GROUP)
      return(rbind(rowNA("mid"), rowNA("high")))

    sub[, mid  := as.numeric(get(grp_col) == "mid")]
    sub[, high := as.numeric(get(grp_col) == "high")]

    design_cols <- c(t_col, e_col, "mid", "high", covars)
    for (c in design_cols) sub[[c]] <- suppressWarnings(as.numeric(sub[[c]]))
    bad <- Reduce(`|`, lapply(design_cols, function(c) is.na(sub[[c]]) | is.infinite(sub[[c]])))
    if (any(bad)) sub <- sub[!bad]
    if (nrow(sub) == 0) return(rbind(rowNA("mid"), rowNA("high")))

    fml <- as.formula(sprintf("Surv(%s, %s) ~ mid + high + %s",
                              t_col, e_col, paste(covars, collapse = " + ")))
    fit <- tryCatch(coxph(fml, data = sub, ties = TIES_METHOD), error = function(e) e)
    if (inherits(fit, "error")) return(rbind(rowNA("mid"), rowNA("high")))

    sm <- summary(fit)
    get_line <- function(term_label) {
      if (!(term_label %in% rownames(sm$coefficients))) return(rowNA(term_label))
      hr  <- sm$coefficients[term_label, "exp(coef)"]
      lci <- sm$conf.int[term_label, "lower .95"]
      uci <- sm$conf.int[term_label, "upper .95"]
      p   <- sm$coefficients[term_label, "Pr(>|z|)"]
      total_term <- sum(sub[[grp_col]] == term_label, na.rm = TRUE)
      case_term  <- sum(sub[[grp_col]] == term_label & sub[[e_col]] == 1, na.rm = TRUE)
      data.frame(exposure = sub("_100$", "", score_100_col), term = term_label, outcome = pfx,
                 total = as.integer(total_term), case = as.integer(case_term),
                 HR = hr, LCI = lci, UCI = uci, P = p, HR_text = fmt_hr(hr, lci, uci),
                 stringsAsFactors = FALSE)
    }
    rbind(get_line("mid"), get_line("high"))
  }

  ### Task grid + parallel
  score_100_cols <- score_100_cols[score_100_cols %in% names(dt)]
  task_grid <- CJ(score = score_100_cols, i = seq_len(nrow(eps)))

  plan(multisession, workers = N_WORKERS)
  res_list <- future_lapply(seq_len(nrow(task_grid)), function(k) {
    run_one(task_grid$score[k], eps[task_grid$i[k]])
  })
  plan(sequential)

  res <- rbindlist(res_list, fill = TRUE)
  setorder(res, exposure, outcome, term)

  ### HR matrix: disease × score (mid / high / p triplets)
  hr_mat <- res %>%
    dplyr::select(outcome, exposure, term, HR_text) %>%
    tidyr::pivot_wider(id_cols = c(outcome, exposure),
                       names_from = term, values_from = HR_text)

  p_mat <- res %>%
    dplyr::filter(term == "high") %>%
    dplyr::select(outcome, exposure, p = P)

  mat <- hr_mat %>%
    dplyr::left_join(p_mat, by = c("outcome", "exposure")) %>%
    dplyr::mutate(
      mid  = as.character(mid),  high = as.character(high),
      p    = ifelse(is.na(p), NA_character_, formatC(p, format = "f", digits = 4))
    ) %>%
    tidyr::pivot_longer(cols = c(mid, high, p), names_to = "piece", values_to = "val") %>%
    dplyr::mutate(colname = paste0(exposure, "_", piece)) %>%
    dplyr::select(outcome, colname, val) %>%
    tidyr::pivot_wider(id_cols = outcome, names_from = colname, values_from = val)

  ord_scores <- sort(unique(res$exposure))
  ordered_cols <- c("outcome",
    as.vector(rbind(paste0(ord_scores, "_mid"),
                    paste0(ord_scores, "_high"),
                    paste0(ord_scores, "_p"))))
  ordered_cols <- intersect(ordered_cols, names(mat))
  final_matrix <- mat[, ordered_cols, drop = FALSE]

  out_file <- file.path(OUT_ROOT, "UKB_cox_HR_matrix.csv")
  safe_write_csv(as.data.table(final_matrix), out_file)
  message("UKB Cox done: ", out_file)

  invisible(list(dt = dt, res = res, eps = eps, covars = covars, score_100_cols = score_100_cols))
}

###### KM Curves (Fig 2b) ------

plot_km_curves <- function(ukb_res, outcome_prefix = NULL) {
  message("===== KM Curves (Fig 2b) =====")
  dt  <- ukb_res$dt
  eps <- ukb_res$eps

  if (is.null(outcome_prefix)) outcome_prefix <- eps$prefix[1]
  t_col <- paste0(outcome_prefix, "_time")
  e_col <- paste0(outcome_prefix, "_incident")
  b_col <- paste0(outcome_prefix, "_baseline")

  llm_grp_col <- grep("overall_diet_score_100_grp", names(dt), value = TRUE)[1]
  if (is.na(llm_grp_col)) llm_grp_col <- grep("_100_grp", names(dt), value = TRUE)[1]
  stopifnot(!is.na(llm_grp_col))

  sub <- dt[dt[[b_col]] == 0]
  sub <- sub[!is.na(sub[[t_col]]) & !is.na(sub[[e_col]]) & !is.na(sub[[llm_grp_col]])]
  sub[[t_col]] <- as.numeric(sub[[t_col]])
  sub[[e_col]] <- as.integer(sub[[e_col]] > 0)

  fit <- survfit(as.formula(sprintf("Surv(%s, %s) ~ %s", t_col, e_col, llm_grp_col)),
                 data = sub)

  p <- ggsurvplot(fit, data = sub,
                  pval = TRUE, pval.coord = c(0, 0.15),
                  palette = c("#2E9FDF", "#E7B800", "#FC4E07"),
                  legend.labs = c("Low (bottom 10%)", "Mid (45-55%)", "High (top 10%)"),
                  xlab = "Follow-up time (years)", ylab = "Survival probability",
                  ggtheme = theme_minimal(base_family = "Arial"),
                  risk.table = TRUE, risk.table.height = 0.25)

  ggsave(file.path(OUT_ROOT, paste0("Fig2b_KM_", outcome_prefix, ".pdf")),
         plot = print(p), width = 8, height = 6, device = "pdf")
  message("Fig 2b saved")
}

###### Disease-Specific Score Cox (Fig 2c) ------

run_disease_specific_cox <- function(ukb_res) {
  message("===== Disease-Specific Score Cox (Fig 2c) =====")
  dt    <- ukb_res$dt
  eps   <- ukb_res$eps
  covars <- ukb_res$covars

  ### Find disease_specific score columns (naming convention: *_specific_*_score)
  spec_cols <- grep("specific.*_score$", names(dt), value = TRUE)
  overall_col <- grep("overall_diet_score$", names(dt), value = TRUE)[1]
  if (is.na(overall_col)) overall_col <- grep("overall.*_score$", names(dt), value = TRUE)[1]

  if (!length(spec_cols) || is.na(overall_col)) {
    message("  No disease_specific_score columns found; check column naming convention")
    return(invisible(NULL))
  }

  spec_100 <- paste0(spec_cols, "_100")
  overall_100 <- paste0(overall_col, "_100")

  res_list <- list()
  for (i in seq_len(nrow(eps))) {
    pfx     <- eps$prefix[i]
    t_col   <- paste0(pfx, "_time")
    e_col   <- paste0(pfx, "_incident")
    b_col   <- paste0(pfx, "_baseline")

    ### Overall score → HR
    grp_overall <- paste0(overall_100, "_grp")
    if (!all(c(grp_overall, t_col, e_col) %in% names(dt))) next

    sub <- dt[dt[[b_col]] == 0]
    sub <- sub[!is.na(sub[[grp_overall]]) & !is.na(sub[[t_col]]) & !is.na(sub[[e_col]])]
    if (nrow(sub) < 100) next

    sub[[t_col]] <- as.numeric(sub[[t_col]])
    sub[[e_col]] <- as.integer(sub[[e_col]] > 0)
    sub$high <- as.numeric(sub[[grp_overall]] == "high")

    fit_o <- tryCatch(
      coxph(as.formula(paste0("Surv(", t_col, ", ", e_col, ") ~ high + ",
                              paste(covars, collapse = " + "))),
            data = sub, ties = "efron"),
      error = function(e) NULL)
    if (is.null(fit_o)) next
    sm_o <- summary(fit_o)
    hr_o <- sm_o$coefficients["high", "exp(coef)"]

    ### Disease-specific scores → HR
    for (sc in spec_100) {
      grp_spec <- paste0(sc, "_grp")
      if (!grp_spec %in% names(dt)) next
      sub_s <- dt[dt[[b_col]] == 0]
      sub_s <- sub_s[!is.na(sub_s[[grp_spec]]) & !is.na(sub_s[[t_col]]) & !is.na(sub_s[[e_col]])]
      sub_s[[t_col]] <- as.numeric(sub_s[[t_col]])
      sub_s[[e_col]] <- as.integer(sub_s[[e_col]] > 0)
      sub_s$high <- as.numeric(sub_s[[grp_spec]] == "high")

      fit_s <- tryCatch(
        coxph(as.formula(paste0("Surv(", t_col, ", ", e_col, ") ~ high + ",
                                paste(covars, collapse = " + "))),
              data = sub_s, ties = "efron"),
        error = function(e) NULL)
      if (is.null(fit_s)) next
      sm_s <- summary(fit_s)
      hr_s <- sm_s$coefficients["high", "exp(coef)"]

      res_list[[length(res_list) + 1]] <- data.frame(
        outcome = pfx,
        score   = sub("_100$", "", sc),
        HR_overall = hr_o,
        HR_specific = hr_s,
        stringsAsFactors = FALSE
      )
    }
  }

  if (!length(res_list)) { message("  No valid comparisons"); return(invisible(NULL)) }
  res_spec <- dplyr::bind_rows(res_list) %>%
    dplyr::mutate(HR_ratio = HR_specific / HR_overall)

  out_file <- file.path(OUT_ROOT, "Fig2c_disease_specific_vs_overall.csv")
  safe_write_csv(as.data.table(res_spec), out_file)
  message("Fig 2c results saved: ", out_file)
  invisible(res_spec)
}


################################################################################
###### Cross-Cohort Cox Survival Analysis (Fig 3a-b) ------
################################################################################

run_cohort_cox <- function(cohort_name,
                           diet_scores_file,
                           llm_scores_file,
                           cov_file,
                           outcome_file,
                           id_col  = "eid",
                           out_dir = OUT_ROOT,
                           workers = 8L) {

  message(sprintf("===== %s Cohort Cox =====", cohort_name))
  if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

  clean_df <- function(df, id_col) {
    if (!(id_col %in% names(df))) names(df)[1] <- id_col
    df[[id_col]] <- as.character(df[[id_col]]) %>%
      gsub("\\.0+$", "", .) %>% trimws()
    df
  }

  fmt_hr_se <- function(beta, se, pval, mark_sig = TRUE) {
    hr  <- exp(beta); lcl <- exp(beta - 1.96 * se); ucl <- exp(beta + 1.96 * se)
    txt <- sprintf("%.2f (%.2f-%.2f)", hr, lcl, ucl)
    if (mark_sig && !is.na(pval) && pval < 0.05) txt <- paste0(txt, "*")
    txt
  }

  process_cov <- function(x) {
    x_num <- suppressWarnings(as.numeric(x))
    if (sum(!is.na(x_num)) / length(x_num) > 0.6) return(x_num)
    return(as.numeric(as.factor(x)) - 1)
  }

  rank_group_10pct <- function(s) {
    idx <- which(!is.na(s)); n <- length(idx)
    if (n < 50) return(NULL)
    r <- rank(s[idx], ties.method = "first")
    n_low  <- max(1L, floor(0.10 * n)); n_mid <- max(1L, floor(0.10 * n))
    n_high <- max(1L, floor(0.10 * n))
    mid_start <- max(1L, floor(0.45 * n)); mid_end <- min(n, mid_start + n_mid - 1)
    low_idx  <- idx[r <= n_low]; mid_idx <- idx[r >= mid_start & r <= mid_end]
    high_idx <- idx[r > (n - n_high)]
    low <- mid <- high <- rep(FALSE, length(s))
    low[low_idx] <- TRUE; mid[mid_idx] <- TRUE; high[high_idx] <- TRUE
    list(low = low, mid = mid, high = high,
         low_n = sum(low), mid_n = sum(mid), high_n = sum(high))
  }

  ### Build score matrix: LLM scores primary, diet indices left-joined
  llm  <- clean_df(read_any(llm_scores_file), id_col)
  diet <- clean_df(read_any(diet_scores_file), id_col)
  ren_llm  <- setdiff(names(llm),  id_col)
  ren_diet <- setdiff(names(diet), id_col)
  names(llm)[match(ren_llm,  names(llm))]  <- paste0("LLM_",  ren_llm)
  names(diet)[match(ren_diet, names(diet))] <- paste0("DIET_", ren_diet)
  score_df <- llm %>% dplyr::left_join(diet, by = id_col)
  message(sprintf("  Baseline N (LLM scores): %d", nrow(score_df)))

  cov_df <- clean_df(read_any(cov_file), id_col)
  out_df <- clean_df(read_any(outcome_file), id_col)

  cov_cols <- setdiff(names(cov_df), id_col)
  for (c in cov_cols) cov_df[[c]] <- process_cov(cov_df[[c]])

  score_cols_all <- setdiff(names(score_df), id_col)
  score_ids <- score_df[[id_col]]
  cov_df <- cov_df %>% dplyr::filter(.data[[id_col]] %in% score_ids)
  out_df <- out_df %>% dplyr::filter(.data[[id_col]] %in% score_ids)

  ### Detect outcomes (supports _time/_event and _time/_incident naming)
  outcome_names <- names(out_df)
  diseases_time  <- outcome_names %>% purrr::keep(~ grepl("_time$", .x)) %>%
    gsub("_time$", "", .)
  diseases_event <- outcome_names %>%
    purrr::keep(~ grepl("_(event|incident)$", .x)) %>%
    gsub("_(event|incident)$", "", .)
  diseases <- intersect(diseases_time, diseases_event) %>% sort()
  if (!length(diseases)) {
    diseases_event <- outcome_names %>%
      purrr::keep(~ grepl("_event$", .x)) %>% gsub("_event$", "", .)
    diseases <- intersect(diseases_time, diseases_event) %>% sort()
  }
  message(sprintf("  Outcomes detected: %d", length(diseases)))

  ### Single-score Cox
  run_one_score <- function(dis, scol, m, min_keep = 30) {
    out_names <- c(paste0(scol, "_mid10%"), paste0(scol, "_high10%"))
    if (!(scol %in% names(m))) return(setNames(list("NA", "NA"), out_names))
    s <- suppressWarnings(as.numeric(m[[scol]]))
    if (length(unique(na.omit(s))) < 3) return(setNames(list("NA", "NA"), out_names))

    g <- rank_group_10pct(s)
    if (is.null(g)) return(setNames(list("NA", "NA"), out_names))

    keep <- (g$low | g$mid | g$high)
    if (sum(keep) < min_keep) return(setNames(list("NA", "NA"), out_names))

    df <- m[keep, c("time", "event", cov_cols), drop = FALSE]
    grp_vec <- rep("0", nrow(df))
    grp_vec[g$mid[keep]]  <- "1"
    grp_vec[g$high[keep]] <- "2"

    if (is_EDII(scol)) {
      df$grp <- factor(grp_vec, levels = c("2", "0", "1"))
      target_mid  <- "grp1"; target_high <- "grp0"
    } else {
      df$grp <- factor(grp_vec, levels = c("0", "1", "2"))
      target_mid  <- "grp1"; target_high <- "grp2"
    }

    for (c in cov_cols) df[[c]][is.na(df[[c]])] <- median(df[[c]], na.rm = TRUE)

    hr_mid <- hr_high <- "NA"
    try({
      fit <- coxph(Surv(time, event) ~ grp + ., data = df, ties = "efron")
      sm  <- summary(fit)
      coef_tbl <- as.data.frame(sm$coefficients)
      if (target_mid %in% rownames(coef_tbl))
        hr_mid <- fmt_hr_se(coef_tbl[target_mid, "coef"],
                            coef_tbl[target_mid, "se(coef)"],
                            coef_tbl[target_mid, "Pr(>|z|)"])
      if (target_high %in% rownames(coef_tbl))
        hr_high <- fmt_hr_se(coef_tbl[target_high, "coef"],
                             coef_tbl[target_high, "se(coef)"],
                             coef_tbl[target_high, "Pr(>|z|)"])
    }, silent = TRUE)
    setNames(list(hr_mid, hr_high), out_names)
  }

  ### Single disease
  run_one_dis <- function(dis) {
    time_col <- paste0(dis, "_time")
    ev_names <- names(out_df)
    ev_col   <- ev_names[grepl(paste0(dis, "_(event|incident)"), ev_names)][1]
    if (is.na(ev_col)) {
      ev_col <- paste0(dis, "_incident")
      if (!ev_col %in% ev_names) ev_col <- paste0(dis, "_event")
    }
    if (!all(c(time_col, ev_col) %in% ev_names)) return(tibble(disease = dis))

    tmp <- out_df[, c(id_col, time_col, ev_col)]
    names(tmp)[match(c(time_col, ev_col), names(tmp))] <- c("time", "event")

    m <- tmp %>%
      dplyr::filter(!is.na(time), !is.na(event)) %>%
      dplyr::inner_join(score_df, by = id_col) %>%
      dplyr::left_join(cov_df[, c(id_col, cov_cols), drop = FALSE], by = id_col)

    if (nrow(m) == 0L) return(tibble(disease = dis))
    score_cols <- intersect(score_cols_all, names(m))
    if (length(score_cols) == 0L) return(tibble(disease = dis))

    res_list <- future_lapply(score_cols, function(sc) run_one_score(dis, sc, m))
    row <- list(disease = dis)
    for (k in seq_along(res_list)) row <- c(row, res_list[[k]])
    as_tibble(row)
  }

  ### Main loop
  plan(multisession, workers = workers)
  on.exit(plan(sequential), add = TRUE)

  rows <- vector("list", length(diseases))
  pb <- txtProgressBar(min = 0, max = length(diseases), style = 3)
  for (i in seq_along(diseases)) {
    rows[[i]] <- run_one_dis(diseases[i])
    setTxtProgressBar(pb, i)
  }
  close(pb)

  res <- dplyr::bind_rows(rows)
  out_csv <- file.path(out_dir, sprintf("HR_10pct_summary_%s.csv", cohort_name))
  readr::write_csv(res, out_csv)
  message(sprintf("  %s done: %s", cohort_name, out_csv))

  ### Console summary
  if (nrow(res) > 0) {
    long <- res %>%
      tidyr::pivot_longer(-disease, names_to = "score_contrast", values_to = "hr_txt") %>%
      dplyr::filter(!is.na(hr_txt), hr_txt != "NA") %>%
      dplyr::mutate(score = sub("_(mid10%|high10%)$", "", score_contrast),
                    contrast = sub("^.*_(mid10%|high10%)$", "\\1", score_contrast))

    parsed <- t(vapply(long$hr_txt, parse_hr_row, c(hr = 0, lcl = 0, ucl = 0, sig = FALSE)))
    parsed <- as.data.frame(parsed)
    parsed$sig <- as.logical(parsed$sig)
    long <- dplyr::bind_cols(long, parsed)
    long$ref_group <- ifelse(is_EDII(long$score), "high", "low")
    main <- long %>% dplyr::filter(contrast == "high10%")

    desc_tab <- main %>%
      dplyr::group_by(score, ref_group) %>%
      dplyr::summarise(n = n(),
                       sig_protect = sum(sig & hr < 1, na.rm = TRUE),
                       sig_harm   = sum(sig & hr > 1, na.rm = TRUE),
                       .groups = "drop")
    cat(sprintf("\n===== %s Score × Outcome Summary =====\n", cohort_name))
    print(desc_tab)
  }
  invisible(res)
}

### Cohort entry points
run_nhanes_cox <- function() {
  run_cohort_cox("NHANES",
    diet_scores_file = file.path(NHANES_ROOT, "scores", "otherdietscore.csv"),
    llm_scores_file  = file.path(NHANES_ROOT, "scores", "LLM_score.xlsx"),
    cov_file         = file.path(NHANES_ROOT, "covariates_mice_imputed.csv"),
    outcome_file     = file.path(NHANES_ROOT, "NHANES_outcome.csv"),
    id_col           = "SEQN")
}

run_chns_cox <- function() {
  run_cohort_cox("CHNS",
    diet_scores_file = file.path(CHNS_ROOT, "scores", "CHNS_OTHERDIET.xlsx"),
    llm_scores_file  = file.path(CHNS_ROOT, "scores", "LLM_score.xlsx"),
    cov_file         = file.path(CHNS_ROOT, "covariates_mice_imputed.csv"),
    outcome_file     = file.path(CHNS_ROOT, "death.csv"),
    id_col           = "ID")
}

run_hrs_cox <- function() {
  run_cohort_cox("HRS",
    diet_scores_file = file.path(HRS_ROOT, "scores", "HRS_OTHERDIET.xlsx"),
    llm_scores_file  = file.path(HRS_ROOT, "scores", "LLM_score.xlsx"),
    cov_file         = file.path(HRS_ROOT, "covariates_mice_imputed.csv"),
    outcome_file     = file.path(HRS_ROOT, "death.csv"),
    id_col           = "ID")
}

### CLHLS (n=11,950, age ≥65) — simplified FFQ, all-cause mortality
run_clhls_cox <- function() {
  run_cohort_cox("CLHLS",
    diet_scores_file = file.path(CLHLS_ROOT, "scores", "CLHLS_OTHERDIET.xlsx"),
    llm_scores_file  = file.path(CLHLS_ROOT, "scores", "LLM_score.xlsx"),
    cov_file         = file.path(CLHLS_ROOT, "covariates_mice_imputed.csv"),
    outcome_file     = file.path(CLHLS_ROOT, "death.csv"),
    id_col           = "ID")
}

### XMC (n=30,882, age 35-74) — logistic regression (not Cox), see separate script
run_xmc_cox <- function() {
  run_cohort_cox("XMC",
    diet_scores_file = file.path(XMC_ROOT, "scores", "XMC_OTHERDIET.xlsx"),
    llm_scores_file  = file.path(XMC_ROOT, "scores", "LLM_score.xlsx"),
    cov_file         = file.path(XMC_ROOT, "covariates_mice_imputed.csv"),
    outcome_file     = file.path(XMC_ROOT, "XMC_outcome.csv"),
    id_col           = "ID")
}


################################################################################
###### Repeated Assessment → C-index (Fig 3c) ------
################################################################################

plot_repeated_assessment_cindex <- function(cindex_data_file = NULL) {
  message("===== Repeated Assessment C-index (Fig 3c) =====")

  if (is.null(cindex_data_file)) {
    cindex_data_file <- file.path(UKB_ROOT, "repeated_assessment_cindex.csv")
  }
  cindex_df <- readr::read_csv(cindex_data_file, show_col_types = FALSE)

  ### Expected columns: n_assessments, outcome, c_index, ci_lower, ci_upper
  p <- ggplot(cindex_df, aes(x = n_assessments, y = c_index, color = outcome)) +
    geom_line(linewidth = 1.2) +
    geom_point(size = 2.5) +
    geom_errorbar(aes(ymin = ci_lower, ymax = ci_upper), width = 0.15, linewidth = 1) +
    scale_x_continuous(breaks = 1:3) +
    labs(x = "Number of repeated dietary assessments",
         y = "C-index (95% CI)",
         color = "Health outcome") +
    theme_minimal(base_family = "Arial") +
    theme(panel.border = element_rect(fill = NA, colour = "black", linewidth = 0.6),
          legend.position = c(0.15, 0.85),
          legend.background = element_rect(fill = "white", colour = "grey80"),
          axis.title = element_text(size = 13),
          axis.text  = element_text(size = 11))

  ggsave(file.path(OUT_ROOT, "Fig3c_repeated_assessment_cindex.pdf"),
         p, width = 7, height = 5.5, device = "pdf")
  message("Fig 3c saved")
  p
}


################################################################################
###### Main Entry ------
################################################################################

### UKB main analysis
ukb_res <- run_ukb_cox()
plot_km_curves(ukb_res)
run_disease_specific_cox(ukb_res)

### Cross-cohort validation (uncomment to run)
# run_nhanes_cox()
# run_chns_cox()
# run_hrs_cox()
# run_clhls_cox()
# run_xmc_cox()

### Repeated assessment C-index
# plot_repeated_assessment_cindex()

message("===== 01_Survival_Analysis.R done =====")
