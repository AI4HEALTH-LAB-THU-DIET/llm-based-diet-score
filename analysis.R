# ===== CHNS: compare diet-score files and run Cox models on the LLM-based cohort =====
options(stringsAsFactors = FALSE)
suppressPackageStartupMessages({
  library(tidyverse)
  library(readxl)
  library(survival)
  library(future.apply)
})

# ========= 1) Paths and parallel settings =========
# Comparator diet scores: Excel with columns such as case_id, MED, AHEI, HEI2020, PDI, MIND, PHDI
DIET_SCORES_FILE <- file.path("data", "chns", "CHNS_OTHERDIET.xlsx")

# LLM-based score file: Excel with columns such as ID, overall diet score
LLM_SCORES_FILE <- file.path("data", "chns", "total.xlsx")

# Covariates: the first column should be the participant ID
COV_CSV <- file.path("data", "chns", "covariates_mice_imputed.csv")

# Outcomes: columns such as IDind, death_time, death_event
OUTCOME_CSV <- file.path("data", "chns", "baseline_death_outcome.csv")

OUT_DIR <- file.path("outputs", "chns")
WORKERS <- 8L
DO_DEBUG <- TRUE

ID_COL <- "ID"

if (!dir.exists(OUT_DIR)) dir.create(OUT_DIR, recursive = TRUE)


# ========= 2) Helper functions =========
read_any <- function(path) {
  ext <- tolower(tools::file_ext(path))
  if (ext %in% c("xlsx", "xls")) {
    readxl::read_excel(path)
  } else {
    suppressWarnings(readr::read_csv(path, show_col_types = FALSE, guess_max = 1e5))
  }
}

clean_df <- function(df) {
  if (!(ID_COL %in% names(df))) names(df)[1] <- ID_COL
  df[[ID_COL]] <- df[[ID_COL]] %>% as.character() %>% gsub("\\.0+$", "", .) %>% trimws()
  df
}

fmt_hr <- function(beta, se, pval, mark_sig = TRUE) {
  hr <- exp(beta)
  lcl <- exp(beta - 1.96 * se)
  ucl <- exp(beta + 1.96 * se)
  txt <- sprintf("%.2f (%.2f–%.2f)", hr, lcl, ucl)
  if (mark_sig && pval < 0.05) txt <- paste0(txt, "*")
  txt
}

is_EDII <- function(score_col) grepl("(^|[^A-Za-z])E[._-]?DII", score_col, ignore.case = TRUE)


# ========= 3) Strict low / middle / high 10 percent rank grouping =========
rank_group_10pct <- function(score_vector) {
  idx <- which(!is.na(score_vector))
  n <- length(idx)
  if (n < 50) return(NULL)

  ranks <- rank(score_vector[idx], ties.method = "first")

  n_low <- max(1L, floor(0.10 * n))
  n_mid <- max(1L, floor(0.10 * n))
  n_high <- max(1L, floor(0.10 * n))

  mid_start <- max(1L, floor(0.45 * n))
  mid_end <- min(n, mid_start + n_mid - 1)

  low_idx <- idx[ranks <= n_low]
  mid_idx <- idx[ranks >= mid_start & ranks <= mid_end]
  high_idx <- idx[ranks > (n - n_high)]

  low <- mid <- high <- rep(FALSE, length(score_vector))
  low[low_idx] <- TRUE
  mid[mid_idx] <- TRUE
  high[high_idx] <- TRUE

  list(
    low = low,
    mid = mid,
    high = high,
    low_n = sum(low),
    mid_n = sum(mid),
    high_n = sum(high)
  )
}


# ========= 4) Build the score matrix =========
build_score_matrix_twofiles <- function() {
  llm <- read_any(LLM_SCORES_FILE) %>% clean_df()
  diet <- read_any(DIET_SCORES_FILE) %>% clean_df()

  llm_cols <- setdiff(names(llm), ID_COL)
  diet_cols <- setdiff(names(diet), ID_COL)

  names(llm)[match(llm_cols, names(llm))] <- paste0("LLM_", llm_cols)
  names(diet)[match(diet_cols, names(diet))] <- paste0("DIET_", diet_cols)

  master <- llm %>% left_join(diet, by = ID_COL)
  message(">> Baseline LLM cohort size: ", nrow(master))
  master
}


# ========= 5) Load data =========
score_df <- build_score_matrix_twofiles()
cov_df <- clean_df(read_any(COV_CSV))
out_df <- clean_df(read_any(OUTCOME_CSV))

# Automatically coerce covariates:
# numeric-like columns stay numeric; otherwise use integer factor encoding.
COV_COLS <- setdiff(names(cov_df), ID_COL)

process_cov <- function(x) {
  x_num <- suppressWarnings(as.numeric(x))

  if (sum(!is.na(x_num)) / length(x_num) > 0.6) {
    return(x_num)
  }

  factor_x <- as.factor(x)
  as.numeric(factor_x) - 1
}

for (column in COV_COLS) {
  cov_df[[column]] <- process_cov(cov_df[[column]])
}

SCORE_COLS_ALL <- setdiff(names(score_df), ID_COL)

score_ids <- score_df[[ID_COL]]
cov_df <- cov_df %>% filter(.data[[ID_COL]] %in% score_ids)
out_df <- out_df %>% filter(.data[[ID_COL]] %in% score_ids)

diseases_time <- names(out_df) %>% keep(~ grepl("_time$", .x)) %>% gsub("_time$", "", .)
diseases_event <- names(out_df) %>% keep(~ grepl("_event$", .x)) %>% gsub("_event$", "", .)
diseases <- intersect(diseases_time, diseases_event) %>% sort()


# ========= 6) Single-score Cox modeling =========
run_one_score <- function(dis, score_col, merged_df,
                          min_unique = 3, min_keep = 30) {

  out_names <- c(paste0(score_col, "_mid10%"), paste0(score_col, "_high10%"))

  if (!(score_col %in% names(merged_df))) return(setNames(list("NA", "NA"), out_names))

  score_values <- suppressWarnings(as.numeric(merged_df[[score_col]]))
  if (length(unique(na.omit(score_values))) < min_unique) return(setNames(list("NA", "NA"), out_names))

  groups <- rank_group_10pct(score_values)
  if (is.null(groups)) return(setNames(list("NA", "NA"), out_names))

  low <- groups$low
  mid <- groups$mid
  high <- groups$high

  keep <- (low | mid | high)
  if (sum(keep) < min_keep) return(setNames(list("NA", "NA"), out_names))

  df <- merged_df[keep, c("time", "event", COV_COLS), drop = FALSE]
  grp_vec <- rep("0", nrow(df))
  grp_vec[mid[keep]] <- "1"
  grp_vec[high[keep]] <- "2"

  if (is_EDII(score_col)) {
    df$grp <- factor(grp_vec, levels = c("2", "0", "1"))
    target_mid <- "grp1"
    target_high <- "grp0"
  } else {
    df$grp <- factor(grp_vec, levels = c("0", "1", "2"))
    target_mid <- "grp1"
    target_high <- "grp2"
  }

  for (column in COV_COLS) df[[column]][is.na(df[[column]])] <- median(df[[column]], na.rm = TRUE)

  hr_mid <- hr_high <- "NA"

  try({
    fit <- coxph(Surv(time, event) ~ grp + ., data = df)
    sm <- summary(fit)
    coef_tbl <- as.data.frame(sm$coefficients)

    if (target_mid %in% rownames(coef_tbl))
      hr_mid <- fmt_hr(
        coef_tbl[target_mid, "coef"],
        coef_tbl[target_mid, "se(coef)"],
        coef_tbl[target_mid, "Pr(>|z|)"]
      )

    if (target_high %in% rownames(coef_tbl))
      hr_high <- fmt_hr(
        coef_tbl[target_high, "coef"],
        coef_tbl[target_high, "se(coef)"],
        coef_tbl[target_high, "Pr(>|z|)"]
      )
  }, silent = TRUE)

  setNames(list(hr_mid, hr_high), out_names)
}


# ========= 7) Single-disease analysis =========
run_one_dis <- function(dis) {
  time_col <- paste0(dis, "_time")
  event_col <- paste0(dis, "_event")

  tmp <- out_df[, c(ID_COL, time_col, event_col)]
  names(tmp)[2:3] <- c("time", "event")

  merged_df <- tmp %>%
    filter(!is.na(time), !is.na(event)) %>%
    inner_join(score_df, by = ID_COL) %>%
    left_join(cov_df, by = ID_COL)

  if (nrow(merged_df) == 0) return(tibble(disease = dis))

  score_cols <- intersect(SCORE_COLS_ALL, names(merged_df))
  res_list <- future_lapply(score_cols, function(score_col) run_one_score(dis, score_col, merged_df))

  row <- list(disease = dis)
  for (i in seq_along(res_list)) row <- c(row, res_list[[i]])
  as_tibble(row)
}


# ========= 8) Debug output: group counts for every score =========
run_debug_checks <- function() {
  out_list <- list()

  for (dis in diseases) {
    time_col <- paste0(dis, "_time")
    event_col <- paste0(dis, "_event")

    tmp <- out_df[, c(ID_COL, time_col, event_col)]
    names(tmp)[2:3] <- c("time", "event")

    merged_df <- tmp %>%
      filter(!is.na(time), !is.na(event)) %>%
      inner_join(score_df, by = ID_COL)

    record <- list(
      disease = dis,
      n_total = nrow(tmp),
      merged = nrow(merged_df)
    )

    for (score_col in SCORE_COLS_ALL) {
      score_values <- suppressWarnings(as.numeric(merged_df[[score_col]]))
      groups <- rank_group_10pct(score_values)

      if (is.null(groups)) {
        record[[paste0(score_col, "_low")]] <- NA
        record[[paste0(score_col, "_mid")]] <- NA
        record[[paste0(score_col, "_high")]] <- NA
      } else {
        record[[paste0(score_col, "_low")]] <- groups$low_n
        record[[paste0(score_col, "_mid")]] <- groups$mid_n
        record[[paste0(score_col, "_high")]] <- groups$high_n
      }
    }

    out_list[[length(out_list) + 1]] <- record
  }

  debug_df <- bind_rows(out_list)
  readr::write_csv(debug_df, file.path(OUT_DIR, "debug_HR_10pct_groups.csv"))
  message(">> Debug export finished: debug_HR_10pct_groups.csv")
}


# ========= 9) Main execution =========
if (DO_DEBUG) run_debug_checks()

plan(multisession, workers = WORKERS)
on.exit(plan(sequential), add = TRUE)

rows <- list()
times <- list()

pb <- txtProgressBar(min = 0, max = length(diseases), style = 3)
for (i in seq_along(diseases)) {
  dis <- diseases[i]
  start_time <- proc.time()[3]
  rows[[i]] <- run_one_dis(dis)
  times[[i]] <- tibble(disease = dis, seconds = round(proc.time()[3] - start_time, 3))
  setTxtProgressBar(pb, i)
}
close(pb)

res <- bind_rows(rows)
readr::write_csv(res, file.path(OUT_DIR, "HR_10pct_summary_death.csv"))
readr::write_csv(bind_rows(times), file.path(OUT_DIR, "timing_per_disease.csv"))

message(">> Cox analysis completed.")


# ========= 10) Automatic console summary =========
parse_hr_row <- function(x) {
  if (is.na(x) || x == "NA") return(c(hr = NA, lcl = NA, ucl = NA, sig = FALSE))
  sig <- grepl("\\*$", x)
  y <- sub("\\*$", "", x)
  match_obj <- regexec("^\\s*([0-9.]+)\\s*\\(([0-9.]+)–([0-9.]+)\\)\\s*$", y)
  values <- regmatches(y, match_obj)[[1]]
  if (length(values) == 4) {
    c(hr = as.numeric(values[2]), lcl = as.numeric(values[3]), ucl = as.numeric(values[4]), sig = sig)
  } else {
    c(hr = NA, lcl = NA, ucl = NA, sig = sig)
  }
}

if (nrow(res) > 0) {
  long <- res %>%
    pivot_longer(-disease, names_to = "score_contrast", values_to = "hr_txt") %>%
    filter(!is.na(hr_txt), hr_txt != "NA") %>%
    mutate(
      score = sub("_(mid10%|high10%)$", "", score_contrast),
      contrast = sub("^.*_(mid10%|high10%)$", "\\1", score_contrast)
    )

  parsed <- t(vapply(long$hr_txt, parse_hr_row, c(hr = 0, lcl = 0, ucl = 0, sig = FALSE)))
  parsed <- as.data.frame(parsed)
  long <- bind_cols(long, parsed)

  long$ref_group <- ifelse(is_EDII(long$score), "high", "low")
  main <- long %>% filter(contrast == "high10%")

  summary_df <- main %>%
    group_by(score, ref_group) %>%
    summarise(
      n = n(),
      sig_protect = sum(sig & hr < 1, na.rm = TRUE),
      sig_harm = sum(sig & hr > 1, na.rm = TRUE),
      med_hr_sig = ifelse(sum(sig) > 0, median(hr[sig], na.rm = TRUE), NA),
      .groups = "drop"
    )

  print(summary_df)
}
