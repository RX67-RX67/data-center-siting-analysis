# Project Tutorial: Data Center Siting Analysis at the County Level

## Table of Contents

1. [Project Background](#1-project-background)
2. [Research Questions](#2-research-questions)
3. [Repository Walkthrough](#3-repository-walkthrough)
4. [Data Collection and Pipeline](#4-data-collection-and-pipeline)
5. [Understanding the Target Variable](#5-understanding-the-target-variable)
6. [Feature Construction](#6-feature-construction)
7. [Preprocessing](#7-preprocessing)
8. [Modeling: The Zero-Inflation Problem](#8-modeling-the-zero-inflation-problem)
9. [Option 1 — LightGBM Tweedie](#9-option-1--lightgbm-tweedie)
10. [Option 2 — Two-Part Hurdle Model](#10-option-2--two-part-hurdle-model)
11. [Model Comparison](#11-model-comparison)
12. [SHAP Interpretation](#12-shap-interpretation)
13. [County Attractiveness Ranking](#13-county-attractiveness-ranking)
14. [Reproducing the Analysis](#14-reproducing-the-analysis)
15. [Key Takeaways](#15-key-takeaways)

---

## 1. Project Background

Data centers are the physical substrate of the modern internet — they host cloud computing, AI model training and inference, streaming, financial systems, and enterprise software. The AI boom since 2023 has caused demand for data center capacity to outpace supply, triggering a wave of siting decisions by hyperscale operators (Amazon, Microsoft, Google, Meta) and co-location providers.

These siting decisions are not random. They reflect structural features of counties: the reliability and cost of power, the depth of the technical labor market, land availability, climate risk, broadband infrastructure, and increasingly, policy attitudes at the local level. Understanding which structural features actually drive these decisions — and quantifying their relative importance — is the central motivation of this project.

The unit of analysis is the **US county** (or county-equivalent). There are 3,138 counties in the final dataset (after removing 11 rows from the raw 3,149 that had data quality issues). The dataset covers all 50 states plus the District of Columbia and Puerto Rico.

---

## 2. Research Questions

This project addresses two related but distinct questions:

**Question 1 — Presence**: Which structural features predict whether a county has *any* data center activity?

**Question 2 — Scale**: Among counties that do have data centers, which features predict how many?

These two questions have different answers, and separating them is one of the core analytical contributions of the project. The two-part hurdle model (Option 2) is specifically designed to answer both questions simultaneously and with separate interpretations.

---

## 3. Repository Walkthrough

### Execution order

The five notebooks must be run in order:

```
notebooks/eda.ipynb                     ← exploratory analysis
notebooks/preprocessing.ipynb           ← clean and transform features
notebooks/modeling_option1_tweedie.ipynb ← single Tweedie model
notebooks/modeling_option2_hurdle.ipynb  ← two-part hurdle model (primary)
notebooks/interpretation.ipynb           ← SHAP analysis and ranking
```

### Key inputs and outputs

| Stage | Input | Output |
|---|---|---|
| Raw pipeline (`scripts/`) | Raw source files in `data/raw_data/` | `data_revealed/03_tables/county_final_table_clean.csv` |
| Preprocessing | `03_tables/county_final_table_clean.csv` (3,149 × 41) | `04_tables/county_preprocessed.csv` (3,138 × 42) |
| Modeling Option 1 | `county_preprocessed.csv` | `models/option1_tweedie.joblib` |
| Modeling Option 2 | `county_preprocessed.csv` | `models/option2_hurdle.joblib` |
| Interpretation | `option2_hurdle.joblib` + `county_preprocessed.csv` | `04_tables/county_attractiveness_ranking.csv` |

---

## 4. Data Collection and Pipeline

### 4.1 Structured data sources

Eight structured datasets are joined at the county level. They are read, cleaned, and transformed by the scripts in `scripts/00_*.py` through `scripts/03_*.py`.

#### Electricity prices (OEDI, ZIP-level)

The electricity price data comes from OEDI and is published at the ZIP code level, separately for investor-owned utilities (IOU) and non-investor-owned utilities (non-IOU). Each record has a commercial rate and an industrial rate in $/kWh.

The raw files are:
- `data/raw_data/electricity_price_2023/iou_zipcodes_2023.csv`
- `data/raw_data/electricity_price_2023/non_iou_zipcodes_2023.csv`

These are merged and aggregated to the county level using the ZIP-to-county crosswalk (see Section 4.3).

#### Grid infrastructure (DOE, county-FIPS level)

The DOE county-level energy employment dataset (`data/raw_data/grid_2023/`) contains employment counts by energy sub-sector for each county. Key sub-sectors used:
- Solar, Wind, Hydroelectric → summed to `clean_energy_jobs`
- Traditional TDS (transmission/distribution/storage), Storage, Smart Grid, Micro Grid → summed to `grid_infrastructure_jobs`
- Natural gas electric power generation → `epg_natural_gas`

A complication: values below 10 are suppressed by BEA disclosure rules and replaced with the string `"<10"`. The pipeline maps `"<10"` → 5 (the midpoint of the [0,10) range) before computing sums.

#### Environmental risk (FEMA National Risk Index, county-level)

The NRI dataset provides 18 hazard-specific risk indices and two composite scores per county. These are read directly with minimal transformation — the raw NRI values are already on a standardized scale.

#### Broadband (FCC, county-FIPS level)

The FCC broadband deployment data (`data/raw_data/Internet_2025/`) gives coverage rates by technology type and download/upload speed tier. The raw data is in long format (one row per technology-speed combination); the pipeline pivots it to wide format to produce features like `fiber_1000_100_coverage` (share of business locations with 1 Gbps+ fiber).

#### Labor costs (BLS, county level)

The BLS Quarterly Census of Employment and Wages dataset contains average weekly wages by industry at the county level. The pipeline filters to three industries:
- Industry 1021: Trade, transportation, and utilities → `wage_trade_transport_utilities`
- Industry 1022: Information → `wage_information`
- Industry 1024: Professional and business services → `wage_prof_business`

#### Land price (AEI, county-FIPS level)

The AEI land price dataset provides a "standardized land value for a 1/4-acre lot" for each county, filtered to the 2023 vintage. Despite the word "standardized" in the column name, this is a **dollar-denominated value** (price per 1/4-acre lot in USD), not a z-score. The typical US county has a median value around $60,000–$90,000 per 1/4-acre plot.

#### Transportation (BTS, county level)

The BTS CTP dataset provides airport counts by FAA classification, rail track counts, and dock/port counts. The pipeline engineers three features from these:
- `air_connectivity`: weighted sum of airports by FAA tier, then log1p
- `rail_intensity`: log1p of rail track count
- `infrastructure_quality`: weighted fraction of good/fair/poor infrastructure
- `dock_presence`: binary (1 if any dock exists)

#### Geographic reference data (Census / HUD, county-FIPS)

Two reference tables are used for geographic alignment:
1. **County FIPS reference** (`data/raw_data/zip_county_transformation/all-geocodes-v2024.xlsx`): maps county names and FIPS codes across all datasets.
2. **ZIP-county crosswalk** (`data/raw_data/zip_county_transformation/ZIP_COUNTY_092025.xlsx`): maps ZIP codes to counties with allocation ratios (see Section 4.3).

### 4.2 Web-scraped data

#### Data center locations

A custom scraper (`src/scraper/scraper.py`, invoked by `scripts/00_pipeline_get_datacenter.py`) collects data center locations from a public aggregator. The output is one CSV per state in `data/processed_data/datacenters_*.csv`, each with at least a ZIP code for each data center.

These are aggregated to ZIP-level counts, then allocated to counties using `business_ratio` from the crosswalk (see Section 4.3).

#### County-level policy information

The policy pipeline has three stages:

**Stage 1 — Search** (`scripts/00_pipeline_get_policy.py`): Runs a set of queries (e.g., "data center county moratorium", "data center county tax incentive") against a search API. For each result, extracts candidate county names from the title and snippet using the pattern `"<Name> County"`. Outputs candidate (county, state, URL) triples to `data/processed_data/county_candidates.csv`.

**Stage 2 — LLM check** (`scripts/00_pipeline_llm_check.py`): For each unique URL, fetches the page text and sends it to an LLM (GPT-4) with a structured prompt asking:
- Is this about a data-center-related policy (ordinance, moratorium, zoning change, tax incentive)?
- Does it support or oppose data center siting?
- Which county and state are mentioned?

Outputs per-URL classifications to `data/processed_data/county_candidates_llm_check.json`.

**Stage 3 — Aggregation** (`scripts/00_llm_check_csv_clean.py`): Cleans the LLM output, maps support direction to {1=support, -1=oppose, 0=neutral}, and aggregates to one row per (state, county):
- `has_policy_signal`: 1 if the county appears in at least one verified policy document
- `policy_direction_score`: mean of support direction scores across all mentions (range −1 to 1)

### 4.3 ZIP-to-county transformation logic

Many source datasets are published at the ZIP code level (electricity prices, data center counts). Converting these to the county level requires a crosswalk.

The HUD ZIP-county crosswalk provides three allocation ratios for each (ZIP, county) pair:
- `residential_ratio`: share of residential addresses in that ZIP that fall in that county
- `business_ratio`: share of business addresses in that ZIP that fall in that county
- `other_ratio`: share of other addresses

**This project uses `business_ratio` as the sole allocation weight.** The rationale: data centers are commercial facilities. Their location is better proxied by where businesses operate than where people live. For a county allocation, the formula is:

```
feature_county = Σ (feature_zip × business_ratio[zip, county])
```

For electricity prices, this produces a weighted-average price across all ZIPs that overlap the county, weighted by the share of business activity in each ZIP. For data center counts, it distributes each ZIP's count across potentially multiple counties proportional to business presence.

This means `num_datacenters` can be **fractional** — a data center in a ZIP code that straddles two counties is allocated as a fraction to each. This is by design: the variable is an *expected count* (a continuous proxy) rather than a discrete observed count. The model handles this correctly under the Tweedie objective.

---

## 5. Understanding the Target Variable

### 5.1 Construction

```
num_datacenters[county] = Σ_{zip ∈ county} count[zip] × business_ratio[zip, county]
```

The result is stored in `data_revealed/03_tables/county_final_table_clean.csv` as `num_datacenters`.

### 5.2 Distribution

After preprocessing (3,138 counties):

| Range | Count | Share |
|---|---|---|
| = 0 (no data centers) | 2,387 | 76.1% |
| (0, 1) fractional | 214 | 6.8% |
| [1, 5) | 367 | 11.7% |
| [5, 10) | 66 | 2.1% |
| [10, 50) | 93 | 3.0% |
| [50, 100) | 6 | 0.2% |
| ≥ 100 | 5 | 0.2% |

Summary statistics: mean = 1.39, max = 304.0 (Loudoun County, VA), non-zero mean = 5.68, non-zero median = 1.00.

### 5.3 Two structural problems

The target distribution has two properties that jointly make standard regression inappropriate:

**Problem 1 — Zero inflation (76.1% zeros)**

Most US counties have no data center activity. This is not a data collection failure — it reflects genuine geographic concentration. Data centers require power, fiber trunk lines, IT labor, and proximity to end users, and most rural counties lack one or more of these. A standard OLS regression applied to this data would try to fit a continuous distribution to a variable with a hard point mass at exactly zero, producing systematically over-predicted values for rural counties.

**Problem 2 — Right-skewed positive tail**

Among the 24% of counties that do have data centers, the distribution is extremely right-skewed. The top 5 counties (Loudoun VA, Maricopa AZ, Cook IL, Dallas TX, Harris TX) account for a disproportionate share of total data center activity. The county at rank 6 has roughly 1/5th the activity of Loudoun County. OLS loss would chase this outlier, producing a model that over-fits the extreme counties at the expense of typical counties with 1–5 data centers.

Together, these two properties define the modeling challenge addressed in Sections 9 and 10.

---

## 6. Feature Construction

The preprocessing starts from a raw working table with 41 columns. After dropping redundant columns and adding engineered features, the final feature matrix has 37 columns (plus the target and identifier columns).

### 6.1 Identifier columns (excluded from modeling)

- `county_fips`: 5-digit FIPS code — excluded because it has a broken encoding for 3 counties (see Section 7.1) and because a raw numeric ID has no meaningful ordinal interpretation for a tree model
- `county`: county name string — excluded
- `state`: state name — **excluded as a confounder** (see Section 7.6)
- `county_key`: composite string key (`state||county`) — excluded from features but used as the join key

### 6.2 Grid infrastructure features

**`epg_natural_gas`** — Employment count in natural gas electric power generation.

This represents the labor force directly tied to natural gas power plants in a county. Data centers need large-scale backup power, and access to natural gas infrastructure (pipelines, generation capacity) enables diesel/gas backup generators at scale. The raw BEA count is log1p-transformed.

**`clean_energy_jobs`** — Sum of solar, wind, and hydroelectric employment.

This proxies the scale of the renewable energy sector in a county. Counties with substantial clean-energy employment tend to have both the physical grid infrastructure (transmission capacity) and the institutional relationships (power purchase agreements, utility partnerships) that data center operators seek for large-scale operations. It also serves as a proxy for "green power availability," which matters for operators with sustainability commitments.

The BEA reports this at the county level but applies a disclosure suppression floor: any county with fewer than 15 clean-energy jobs receives the value 15 (a floor), because reporting the exact count would allow identification of individual employers below the threshold. This creates a data artifact where 47.9% of counties sit at exactly the floor value, making the raw continuous value unreliable for those counties. The preprocessing creates a binary `clean_energy_jobs_above_floor` indicator to capture this threshold explicitly.

**`grid_infrastructure_jobs`** — Sum of traditional TDS (transmission, distribution, storage), battery storage, smart grid, and micro grid employment.

This is the most direct proxy for grid infrastructure capacity and maturity. A county with substantial TDS employment has the physical infrastructure (substations, high-voltage lines, storage facilities) to supply the 10–100+ megawatts that a large data center campus requires. BEA suppression floor is 20 jobs (15.9% of counties at floor).

**`epg_natural_gas`** has a floor of 5 (76.5% of counties at floor), making it the most heavily suppressed of the three grid features.

### 6.3 Broadband features

The FCC data provides broadband coverage by technology type and speed tier. Several combinations are computed, but after collinearity analysis during preprocessing, only two are retained:

**`any_tech_1000_100_coverage`** — Share of business locations in the county with any technology offering ≥1 Gbps download / ≥100 Mbps upload.

**`fiber_availability`** — Share of business locations with fiber at ≥1 Gbps / ≥100 Mbps. This is equivalent to `fiber_1000_100_coverage`.

Four other coverage metrics (`any_tech_100_20`, `cable_fiber_100_20`, `fiber_100_20`, `high_speed_wired`) were dropped in preprocessing because they are near-perfectly correlated with the retained features at the county level.

An important finding from modeling (Section 12): broadband coverage rates at the county level are **not** strong predictors of data center presence or scale. This is because coverage is near-universal (most counties have high broadband coverage by area). The discriminating factor is physical fiber infrastructure below the county level (e.g., specific trunk line routes, carrier-neutral facilities), which is not captured in county-level coverage statistics.

### 6.4 Transportation features

**`air_connectivity`** — Weighted index of airport availability.

Airports serve two functions in data center siting: personnel access (engineers, sales, operations staff) and occasional hardware freight (servers arrive by air when urgency requires). The weighting uses FAA airport classification tiers: Large Hub (weight 5) → Medium Hub (4) → Small Hub (3) → Non-Hub Primary (2.5) → Non-Primary classes (1.5 down to 1). The resulting sum is log1p-transformed.

**`rail_intensity`** — log1p of total rail track count in the county.

Rail infrastructure is relevant for heavy equipment delivery (large UPS units, generators, chiller systems) and, more broadly, as a proxy for industrial connectivity of a county.

**`infrastructure_quality`** — Weighted fraction of infrastructure rated "Good."

Computed as `(Good + 0.5 × Fair) / (Good + Fair + Poor)` across all rated infrastructure assets in the county. A county with 100% "Good" infrastructure scores 1.0; one with all "Poor" scores 0. Missing (no rated infrastructure) → imputed to 0.5.

**`dock_presence`** — Binary: 1 if the county has at least one port or dock.

This is a structural binary indicator for water transport access, relevant for counties near navigable rivers or coastlines. The column is confirmed to have only values 0 and 1 in the raw data. **It is not log1p-transformed** — applying log1p to a binary variable produces 0 (for 0) and log1p(1) = 0.693 (for 1), which is a meaningless rescaling of a nominal feature.

### 6.5 Environmental risk features (NRI)

The FEMA National Risk Index provides 18 hazard-specific indices and 2 composite scores per county, covering: cold wave, drought, earthquake, hail, heat wave, hurricane, ice storm, landslide, lightning, riverine flooding, strong wind, tornado, wildfire, winter weather, community resilience, and community risk factor.

These are included as-is (no engineering beyond the log1p transform applied in preprocessing). They are expected to capture the climate and natural hazard exposure that data center operators must account for in long-term siting decisions (uptime SLAs, insurance costs, physical resilience requirements).

An important interpretive caveat (discussed in Section 12): several hazard risk indices appear as strong *presence* predictors in the model, but this is because hazard risk co-varies geographically with the existing DC market, not because operators prefer risky locations.

### 6.6 Land price feature

**`land_value_1_4_acre_standardized`** — Dollar-denominated land value per 1/4-acre lot (2023).

Despite the word "standardized" in the column name, this is a dollar value — it has been standardized to a common lot size (1/4 acre) to allow comparability across counties, not z-score normalized. The median value across counties is approximately $60,000–$90,000.

The raw data contains 895 counties (28.5%) with negative land values, which are assessor data artifacts (data entry errors, appeals in progress, inherited negative adjustments). These are treated as missing values, imputed with the county median, and flagged with a `land_value_missing` binary indicator.

### 6.7 Labor cost features

Three BLS wage features are included, all representing average annual weekly wages for private-sector employees:

**`wage_information`** — IT/media/communications sector wages.

This is the most important wage feature. It proxies the depth and quality of the local IT labor market — the pool of network engineers, systems administrators, security analysts, and data center operations staff that an operator can hire locally. Higher wages in this sector indicate a more specialized, larger labor pool.

**`wage_trade_transport_utilities`** — Logistics and utilities sector wages.

Captures wages in sectors adjacent to data center operations (utility workers, logistics staff).

**`wage_prof_business`** — Professional and business services wages.

Broader proxy for the professional workforce that supports data center tenants (consulting, managed services, IT services).

23.3% of counties have no BLS data for the Information industry (`wage_information = 0`), indicating no measurable IT sector presence. These zeros are genuine structural zeros, not missing values — they correctly encode "no IT labor market in this county."

### 6.8 Policy features

**`has_policy_signal`** — Binary: 1 if the county has appeared in at least one verified policy document about data center regulation or incentives.

**`policy_direction_score`** — Continuous in [-1, 1]: mean of direction scores (1=support, -1=oppose, 0=neutral) across all verified mentions of that county.

These features rank low in SHAP importance for both stages. The likely explanation: data center policy tends to follow investment activity rather than precede it. Counties that already have DCs attract policy attention (for or against) precisely because they already have activity. Counties that lack DCs rarely have any policy discourse about them.

---

## 7. Preprocessing

The preprocessing notebook (`notebooks/preprocessing.ipynb`) transforms the raw table (3,149 rows × 41 cols) into the modeling-ready table (3,138 rows × 42 cols, zero nulls). Each decision is documented below with the specific reasoning.

### 7.1 Join key: `county_key`

**Problem**: Three counties in the raw table have placeholder FIPS codes (`00000`, `99999`, etc.) instead of real 5-digit FIPS. These arose because the web scraper found data center listings in locations that could not be precisely geocoded to a standard county FIPS.

**Solution**: Create a composite string key:
```python
df["county_key"] = df["state"] + "||" + df["county"]
```
This key is verified to be unique across all 3,149 rows (confirmed with `assert df["county_key"].nunique() == len(df)`). It becomes the primary join key for the final table and for any downstream merges.

### 7.2 Drop redundant broadband columns

**Problem**: The raw table contains 6 broadband coverage features. EDA revealed that several pairs are near-perfectly correlated (Pearson r > 0.97) at the county level:

- `any_tech_100_20_coverage` correlates strongly with `any_tech_1000_100_coverage`
- `cable_fiber_100_20_coverage` correlates strongly with `cable_fiber_1000_100_coverage` (retained as `high_speed_wired_coverage`)
- `fiber_100_20_coverage` correlates with `fiber_1000_100_coverage` (retained as `fiber_availability`)

**Solution**: Drop `any_tech_100_20_coverage`, `cable_fiber_100_20_coverage`, `fiber_100_20_coverage`, and `high_speed_wired_coverage`. Retain `any_tech_1000_100_coverage` and `fiber_availability`. This reduces multicollinearity without losing information.

### 7.3 BEA suppression floor indicators

**Problem**: The BEA employment counts for energy sectors (`epg_natural_gas`, `clean_energy_jobs`, `grid_infrastructure_jobs`) have disclosure suppression floors: any county below the threshold receives a fixed minimum value rather than the true count. This creates artificial point masses at the floor values:

- `epg_natural_gas`: floor = 5 employees → 76.5% of counties at exactly this value
- `clean_energy_jobs`: floor = 15 employees → 47.9% of counties at exactly this value
- `grid_infrastructure_jobs`: floor = 20 employees → 15.9% of counties at exactly this value

If the continuous feature alone is fed to the model, the log1p-transformed floor value (e.g., `log1p(15) = 2.773` for `clean_energy_jobs`) is indistinguishable from a county that genuinely has exactly 15 clean-energy employees. But the information content is completely different: floor counties have *at most* 15 jobs (we don't know the true value), while above-floor counties have *at least* 16 (and the exact count is known).

**Solution**: Add binary above-floor indicators *before* any transforms, directly on the raw values:

```python
FLOOR_SPECS = {
    "epg_natural_gas":           5.0,
    "clean_energy_jobs":         15.0,
    "grid_infrastructure_jobs":  20.0,
}
for col, floor_val in FLOOR_SPECS.items():
    df[col + "_above_floor"] = (df[col] > floor_val).astype(int)
```

The continuous feature (after log1p) captures the *magnitude* of the energy sector; the binary indicator captures *whether meaningful data exists at all*. Both are needed because LightGBM tree splits can handle both simultaneously: one split at the floor boundary, and finer splits above the floor.

### 7.4 Land value treatment

**Problem**: The raw `land_value_1_4_acre_standardized` column contains 895 negative values (28.5% of counties). These cannot represent real land prices in dollar terms. Investigation confirms they are assessor data artifacts (data entry errors, pending appeals, or inherited negative adjustments from prior survey rounds) and not a data encoding where negative means something meaningful.

**Solution** (applied in this exact order):

```python
# Step 1: Replace negatives with NaN BEFORE creating the flag
df.loc[df["land_value_1_4_acre_standardized"] < 0, "land_value_1_4_acre_standardized"] = np.nan

# Step 2: Create binary flag for imputed values
df["land_value_missing"] = df["land_value_1_4_acre_standardized"].isna().astype(int)

# Step 3: Impute with median (computed after removing negatives)
median_val = df["land_value_1_4_acre_standardized"].median()
df["land_value_1_4_acre_standardized"] = df["land_value_1_4_acre_standardized"].fillna(median_val)
```

The flag `land_value_missing` preserves the information that the value was imputed. The model can then learn that "county with imputed land value" is a distinct structural state (typically rural counties with limited assessor data).

**Critical ordering note**: The flag must be created from the NaN column, not the original negatives. If the order were reversed (flag from original, then convert to NaN), the flag would capture counties with negative values — but we want it to capture *all* missing values including any pre-existing NaNs in the raw data.

### 7.5 Electricity price treatment

**Problem**: 15+ counties have `industrial_price = 0.0` exactly, and an additional handful have values below $0.01/kWh. These are not legitimate prices — they arise from counties where no utility reports prices (zero = "not reported"), or from data entry errors in the utility filings.

**Solution**: Apply a threshold below which values are treated as missing:

```python
PRICE_MIN_THRESHOLD = 0.01  # $/kWh — below this is implausible
for col in ["commercial_price", "industrial_price"]:
    df.loc[df[col] < PRICE_MIN_THRESHOLD, col] = np.nan
    df[col] = df[col].fillna(df[col].median())
```

The median imputation is reasonable here because electricity prices have a relatively tight distribution (most US counties fall in $0.06–$0.14/kWh for commercial electricity). The median is robust to the extreme outliers on both ends.

### 7.6 Exclude `state` from the feature matrix

**Decision**: `state` is excluded from the feature matrix, even though it is available.

**Rationale**: `state` is a geographic label, not a structural cause. Including it would allow the model to learn "Virginia → many data centers" and assign SHAP credit to the state label rather than to the structural features that make Virginia attractive (abundant grid capacity, deep IT labor market, competitive electricity prices). This would undermine the interpretive goal of the analysis: identifying which *structural features* drive siting decisions.

In practice, excluding `state` barely affects predictive performance (AUC dropped by ~0.4 percentage points when the feature was removed) while dramatically improving interpretability. The hazard risk indices do partially absorb geographic variance after `state` is removed (see Section 12.4 on confounding), but this is a known limitation documented in the interpretation notebook.

### 7.7 `log1p` transform

**Decision**: Apply `log1p(x) = log(1 + x)` to 24 continuous features. Do NOT apply it to binary features.

**Why log1p, not log?**

`log1p` is chosen over `log` because several features have exact-zero values that are legitimate (e.g., a county with zero rail track has `rail_intensity = 0`). `log(0)` is undefined; `log1p(0) = 0`, which preserves the zero as zero.

**Why apply it at all?**

LightGBM finds splits based on the sorted order of feature values. For a highly right-skewed feature (e.g., `wage_information` might range from 0 to $5,000/week with most values clustered below $1,000), most split candidates are in a compressed region. After log1p, the distribution is more uniform, allowing the algorithm to find splits that meaningfully separate low-value from high-value counties without being dominated by extreme outliers.

**The 24 features that receive log1p**:
All 14 NRI hazard risk indices, `grid_infrastructure_jobs`, `clean_energy_jobs`, `epg_natural_gas`, `wage_information`, `wage_prof_business`, `wage_trade_transport_utilities`, `commercial_price`, `industrial_price`, `land_value_1_4_acre_standardized`, `air_connectivity`.

**Features that do NOT receive log1p**:
- `dock_presence`: confirmed binary (0 or 1 only). `log1p(1) = 0.693` is a meaningless rescaling.
- `any_tech_1000_100_coverage`, `fiber_availability`: proportions already in [0, 1].
- `infrastructure_quality`: already a proportion in [0, 1].
- `community_resilience_value`, `community_risk_factor_value`: NRI composite scores already on a normalized scale.
- `has_policy_signal`, `policy_direction_score`: binary/bounded-continuous policy features.
- The `*_above_floor` binary indicators and `land_value_missing`: binary by construction.

### 7.8 Final preprocessed table

`data_revealed/04_tables/county_preprocessed.csv`: 3,138 rows × 42 columns, zero nulls.

New columns added vs. raw table:
- `county_key` (string join key)
- `epg_natural_gas_above_floor` (binary)
- `clean_energy_jobs_above_floor` (binary)
- `grid_infrastructure_jobs_above_floor` (binary)
- `land_value_missing` (binary)

Columns removed vs. raw table:
- `any_tech_100_20_coverage`, `cable_fiber_100_20_coverage`, `fiber_100_20_coverage`, `high_speed_wired_coverage` (4 redundant broadband columns)
- 11 rows dropped (problematic FIPS with no matching geographic data)

---

## 8. Modeling: The Zero-Inflation Problem

Before describing the specific models, it's important to understand why the statistical approach matters here.

### 8.1 Why not standard linear regression?

OLS minimizes the sum of squared residuals. On this dataset:
- It predicts continuous values for all counties, including values like -2 or -0.5 for rural counties (since the model has no way to constrain predictions to ≥ 0)
- The loss is dominated by Loudoun County (304 data centers). The model optimizes heavily to predict that single county well, at the cost of the other 3,137 counties.
- The 76% zero counties bias the model's intercept downward, producing systematically negative or near-zero predictions for most counties.

### 8.2 Why not Poisson regression?

Poisson regression enforces non-negativity (log link) and is designed for count data. But it assumes that the mean equals the variance — a property called "equidispersion." For this dataset, the variance is massively larger than the mean (variance ≈ 71.6, mean ≈ 1.39), a condition called "overdispersion." Poisson regression would produce poorly calibrated confidence intervals and systematically underestimate uncertainty.

Additionally, Poisson does not natively handle the mass of exact zeros well — it assigns some probability to zero (via the Poisson PMF), but not enough to match the 76% zero rate in this data.

### 8.3 Why not negative binomial regression?

Negative binomial extends Poisson to handle overdispersion. It does better, but it still treats all observations as coming from a single distribution, which blurs the distinction between "structural zeros" (counties that will never have a data center under any scenario) and "sampling zeros" (counties that happen to have none yet but have the structural prerequisites).

### 8.4 The two approaches taken

**Option 1 — Tweedie distribution**: A compound Poisson-Gamma distribution that natively handles zero inflation and right-skewed positive tails in a single model. The key parameter `p` (variance power) controls where the model sits between Poisson (p=1) and Gamma (p=2). When `1 < p < 2`, the model assigns positive probability to exact zero AND models the positive tail as Gamma-like.

**Option 2 — Two-part hurdle model**: Explicitly separates the zero-generation process (Stage 1 binary classifier) from the positive-count process (Stage 2 regressor). This gives separate SHAP values for each question.

---

## 9. Option 1 — LightGBM Tweedie

### 9.1 The Tweedie family

The Tweedie distribution with power parameter `p` in (1, 2) is a compound Poisson-Gamma distribution. Concretely:

- A Poisson random variable `N ~ Poisson(λ)` governs how many "events" occur
- Each event generates a Gamma-distributed quantity `G_i ~ Gamma(α, β)`
- The total is `Y = G_1 + G_2 + ... + G_N` (with Y = 0 when N = 0)

When N = 0, Y = 0 exactly — this is the zero-inflation mechanism. When N ≥ 1, Y follows a Gamma-like distribution — this handles the right-skewed positive tail.

The variance of the Tweedie distribution is `Var(Y) = φ · E[Y]^p`, where `φ` is a dispersion parameter. The power `p` controls the mean-variance relationship:
- `p = 1`: variance proportional to mean (Poisson)
- `p = 2`: variance proportional to mean squared (Gamma)
- `p = 1.3`: variance scales as E[Y]^1.3 — appropriate for a moderately overdispersed count

### 9.2 LightGBM's Tweedie objective

LightGBM implements the Tweedie negative log-likelihood as a loss function:

```
L(y, ŷ) = 2 · [ y^(2-p) / ((1-p)(2-p))  −  y · ŷ^(1-p) / (1-p)  +  ŷ^(2-p) / (2-p) ]
```

The model uses a **log link**: it predicts `log(E[y|X])` internally and returns `exp(raw_prediction)` as the output. This guarantees non-negative predictions.

**Why not transform the target with log1p?** Because we already applied log1p to many *features*. The Tweedie objective applies a log link to the *model output*. Transforming the target separately would double-apply the log and destroy the distributional assumption.

### 9.3 Selecting the variance power `p`

The parameter `p` is tuned via 5-fold stratified cross-validation, stratified on `(y > 0)` to preserve the 76/24 zero/non-zero ratio in each fold.

Grid: `p ∈ {1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9}`

Within each fold, LightGBM early stopping on the validation set's Tweedie deviance prevents overfitting and auto-selects the number of trees. The metric is mean Tweedie deviance across folds.

Results:

| p | CV Tweedie deviance |
|---|---|
| 1.1 | 3.150 |
| 1.2 | 2.781 |
| **1.3** | **2.753** ← best |
| 1.4 | 2.876 |
| 1.5 | 3.280 |
| 1.6 | 4.054 |
| 1.7 | 5.200 |
| 1.8 | 7.695 |
| 1.9 | 15.255 |

**Best `p = 1.3`**: the compound Poisson-Gamma regime, slightly tilted toward Poisson. This reflects that the target, while right-skewed, is still closer to a count process (Poisson-like) than a purely multiplicative one (Gamma-like).

### 9.4 Final model configuration

```python
final_model = lgb.LGBMRegressor(
    objective="tweedie",
    tweedie_variance_power=1.3,
    num_leaves=63,
    min_child_samples=20,
    learning_rate=0.05,
    n_estimators=3000,       # capped by early stopping
    random_state=42,
    n_jobs=-1,
)
```

Key hyperparameters:
- `num_leaves=63`: controls tree complexity (2^6 = 64 leaves max). Sufficient for 37 features and 2,510 training rows.
- `min_child_samples=20`: minimum county count per leaf node. Prevents overfitting to small groups of counties.
- `learning_rate=0.05`: relatively slow shrinkage for stable convergence.
- `n_estimators=3000` with early stopping at 75 rounds: the model actually stopped at **iteration 41**, indicating the optimal ensemble is quite small for this dataset.

Training uses an internal 10% early-stopping holdout (stratified, separate from the test set). The remaining 90% of training data is used for actual gradient updates.

### 9.5 Test set results

| Metric | Value |
|---|---|
| Tweedie deviance (p=1.3) | 1.756 |
| MAE (log1p scale) | 0.312 |
| RMSE (log1p scale) | 0.474 |
| AUC (has any DC) | 0.807 |
| Median AE (raw scale) | 0.185 data centers |

The AUC of 0.807 means: if you randomly pick one county that has DCs and one that doesn't, the model's predicted score is higher for the DC county 80.7% of the time. The median absolute error of 0.185 means the typical prediction error is less than 0.2 data centers — quite good given that 76% of counties have exactly zero.

---

## 10. Option 2 — Two-Part Hurdle Model

### 10.1 Motivation

The Tweedie model jointly models presence and scale, but its SHAP values are a single blended signal — they cannot cleanly separate "what makes a county likely to have any DC" from "what makes a DC-having county have more DCs." The two-part hurdle model sacrifices some predictive elegance in exchange for analytical clarity.

### 10.2 Formulation

```
E[y | X]  =  P(y > 0 | X)  ×  E[y | y > 0, X]
              Stage 1            Stage 2
```

Stage 1 produces `P(y > 0 | X)`: the probability that a county has any data center given its structural features.

Stage 2 produces `E[y | y > 0, X]`: the expected count *assuming the county has at least one*. This is estimated from a model trained only on the 767 positive counties (24.4% of all counties).

The combined prediction `P(y>0|X) × E[y|y>0,X]` is the hurdle model's expected count, which accounts for both the probability of presence and the expected scale.

### 10.3 Stage 1: Presence classifier

```python
stage1 = lgb.LGBMClassifier(
    objective="binary",
    metric="auc",
    is_unbalance=True,
    num_leaves=63,
    min_child_samples=20,
    learning_rate=0.05,
    n_estimators=3000,
    random_state=42,
)
```

**Target**: `has_dc = (num_datacenters > 0)` — binary, 1 for 24.4% of counties.

**`is_unbalance=True`**: With 76% class 0 and 24% class 1, a naive classifier would achieve 76% accuracy by always predicting 0. `is_unbalance=True` automatically sets the sample weight for the minority class (has DC) to `n_negative / n_positive ≈ 3.1`. This penalizes missed DC-counties three times as heavily as missed non-DC-counties, pushing the model to discriminate rather than predict the majority class.

**Early stopping metric**: AUC on the internal validation holdout, since we care about ranking (can the model score DC counties higher?) not just accuracy.

Stage 1 results on the test set:
- **AUC-ROC: 0.847** — strong discrimination between DC and non-DC counties
- **Average Precision: 0.701** — area under the precision-recall curve (more informative than AUC for imbalanced classes)
- Confusion matrix at 0.5 threshold: 440 true negatives, 83 true positives, 35 false positives, 70 false negatives

The 70 false negatives (DC counties predicted as non-DC) and 35 false positives (non-DC counties predicted as having DCs) are important. A 0.5 threshold is not necessarily optimal — for county ranking purposes, the predicted probability itself (not the thresholded binary) is used.

### 10.4 Stage 2: Scale regressor (positive counties only)

```python
stage2 = lgb.LGBMRegressor(
    objective="tweedie",
    tweedie_variance_power=1.8,
    num_leaves=31,
    min_child_samples=10,
    learning_rate=0.05,
    n_estimators=3000,
    random_state=42,
)
```

**Training data**: Only the 614 positive counties in the training set (counties with `num_datacenters > 0`).

**Why Tweedie again for Stage 2?** The positive-only target is still right-skewed (non-zero mean = 5.68, median = 1.00, max = 304). The Tweedie objective handles this skew better than MSE. However, since we've removed all zeros from Stage 2's training data, there is no zero inflation at this stage — the distribution is closer to Gamma than to compound Poisson-Gamma.

**Why `p = 1.8` for Stage 2 vs. `p = 1.3` for Option 1?** The CV over positive counties selects `p = 1.8`, which is in the Gamma regime (`p → 2`). This makes sense: without zero inflation in the training data, the optimal variance power shifts toward Gamma, which models the multiplicative right-skew of the positive counts.

Stage 2 uses `num_leaves=31` (smaller than Stage 1's 63) because the training set is smaller (~614 rows), and smaller trees reduce the risk of overfitting.

Stage 2 best iteration: **22 trees** — the model is very shallow, reflecting the limited signal in 614 positive counties across 37 features.

### 10.5 Combined prediction

```python
prob_pos   = stage1.predict_proba(X_test)[:, 1]      # P(y > 0 | X), shape (628,)
scale_pred = np.maximum(stage2.predict(X_test), 1e-9) # E[y | y>0, X], all counties
y_pred     = prob_pos * scale_pred                    # hurdle prediction
```

Note that Stage 2 is applied to **all** test counties, not just the positives. This is conceptually correct: `E[y|y>0,X]` is "what would the count be if this county had DCs?" Stage 1 then weights this by the probability that the scenario is realized. A county with low `prob_pos` but high `scale_pred` would get a low combined score, correctly reflecting that it's unlikely to have DCs even though its structural profile would support scale if it did.

### 10.6 Combined results

| Metric | Value |
|---|---|
| Tweedie deviance (p=1.3, for comparison) | 1.965 |
| MAE (log1p scale) | 0.279 |
| RMSE (log1p scale) | 0.524 |
| AUC (has any DC) | 0.833 |
| Median AE (raw scale) | 0.052 data centers |

---

## 11. Model Comparison

### 11.1 Metrics side by side

| Metric | Option 1 (Tweedie) | Option 2 (Hurdle) | Better |
|---|---|---|---|
| Tweedie deviance (p=1.3) | 1.756 | 1.965 | Option 1 |
| MAE (log1p) | 0.312 | **0.279** | Option 2 |
| RMSE (log1p) | **0.474** | 0.524 | Option 1 |
| AUC (has any DC) | 0.807 | **0.833** | Option 2 |
| Median AE (raw) | 0.185 | **0.052** | Option 2 |

### 11.2 Interpreting the trade-off

**Option 1 wins on Tweedie deviance and RMSE (log scale)**. The Tweedie deviance comparison is slightly unfair to Option 2 because Option 2's combined output is not explicitly optimizing Tweedie deviance — it's optimizing Stage 1 AUC and Stage 2 Tweedie deviance separately. The RMSE advantage for Option 1 on the log scale reflects that Option 2's Stage 2 (trained on only 614 counties) has higher variance in its predictions for high-count counties.

**Option 2 wins on AUC, MAE, and median AE**. AUC (+2.6 pp) reflects that the dedicated binary Stage 1 classifier is better at separating DC from non-DC counties than the joint Tweedie model. MAE and median AE improvements reflect that Option 2 is better calibrated for the majority of counties (those with 0 or few DCs), which account for most of the population of interest for siting analysis.

**The primary advantage of Option 2 is interpretive, not predictive**. Separate SHAP analyses for Stage 1 and Stage 2 answer two structurally different questions, with meaningfully different feature rankings.

---

## 12. SHAP Interpretation

SHAP (SHapley Additive exPlanations) is a method for explaining individual model predictions. For a given county, the SHAP value for a feature tells you how much that feature pushed the prediction above or below the average prediction across all counties. The key properties:

- SHAP values are in the model's output space (log-odds for Stage 1; log-count for Stage 2)
- SHAP values sum to the total deviation from the baseline (expected value)
- Positive SHAP = feature pushes prediction up; Negative SHAP = pushes prediction down
- `mean(|SHAP|)` across all test counties gives global feature importance

For LightGBM, `shap.TreeExplainer` computes exact SHAP values efficiently in O(T × L × N) time where T=trees, L=leaves, N=samples.

### 12.1 Stage 1 SHAP: What drives PRESENCE?

**Top 10 features by mean |SHAP| for Stage 1 (test set, 628 counties):**

| Rank | Feature | Mean |SHAP| | Signed direction | Interpretation |
|---|---|---|---|---|
| 1 | `clean_energy_jobs` | 0.751 | Mixed | Floor vs. above-floor effect |
| 2 | `grid_infrastructure_jobs` | 0.527 | Mixed | Floor vs. above-floor effect |
| 3 | `land_value_1_4_acre_standardized` | 0.369 | Positive | Economic density proxy |
| 4 | `wage_information` | 0.337 | Mixed | IT labor market depth |
| 5 | `lightning_risk_index_value` | 0.252 | Positive | Geographic confounder |
| 6 | `hurricane_risk_index_value` | 0.229 | Positive | Geographic confounder |
| 7 | `tornado_risk_index_value` | 0.213 | Positive | Geographic confounder |
| 8 | `heat_wave_risk_index_value` | 0.198 | Positive | Geographic confounder |
| 9 | `commercial_price` | 0.191 | Positive | Urban market proxy |
| 10 | `land_value_missing` | 0.176 | Negative | Rural/data-scarce county |

### 12.2 Reading SHAP signs for floor-affected features

A key subtlety: the **signed mean SHAP** for `clean_energy_jobs` and `grid_infrastructure_jobs` is negative. This does NOT mean "more clean energy jobs → fewer data centers." It is an artifact of the floor suppression.

Consider `clean_energy_jobs`: 47.9% of counties sit at the floor value `log1p(15) = 2.773`. These are counties with no meaningful clean-energy sector (or a very small one). They are overwhelmingly the zero-DC counties. For these counties, the SHAP value is negative — the feature is pulling DC probability down.

For the 52.1% of counties above the floor (real clean-energy employment), the SHAP value is positive — more clean-energy jobs increases DC probability. The actual relationship is positive above the floor. The *mean* signed SHAP is negative because floor counties outnumber above-floor counties, and floor counties are almost all non-DC.

The **SHAP dependence plot** for `clean_energy_jobs` shows this clearly: a cluster of counties at `x = log1p(15) = 2.773` with negative SHAP values, and an increasing positive relationship above that threshold. The `clean_energy_jobs_above_floor` binary indicator explicitly captures this discontinuity.

### 12.3 Stage 2 SHAP: What drives SCALE?

**Top 10 features by mean |SHAP| for Stage 2 (153 positive test counties):**

| Rank | Feature | Mean |SHAP| | S1 rank | Rank shift |
|---|---|---|---|---|
| 1 | `wage_information` | 0.343 | 4 | ↑ +3 |
| 2 | `grid_infrastructure_jobs` | 0.270 | 2 | Stable |
| 3 | `clean_energy_jobs` | 0.143 | 1 | ↓ −2 |
| 4 | `commercial_price` | 0.093 | 9 | ↑ +5 |
| 5 | `riverine_flooding_risk_index_value` | 0.086 | 13 | ↑ +8 |
| 6 | `industrial_price` | 0.053 | 15+ | ↑ |
| 7 | `land_value_1_4_acre_standardized` | 0.051 | 3 | ↓ −4 |
| 8 | `winter_weather_risk_index_value` | 0.048 | 15+ | ↑ |
| 9 | `epg_natural_gas` | 0.043 | 15+ | ↑ |
| 10 | `hail_risk_index_value` | 0.037 | 15+ | ↑ |

### 12.4 Geographic confounding: the hazard risk story

The most important interpretive caveat in this analysis is the behavior of the NRI hazard risk indices.

In Stage 1, `lightning_risk`, `hurricane_risk`, `tornado_risk`, and `heat_wave_risk` all rank in the top 10 with **positive SHAP values** — counties with higher hazard risk are predicted to be more likely to have data centers. This is counter-intuitive: data center operators presumably prefer lower-risk locations.

The explanation is **geographic confounding**. The three largest US data center markets — Northern Virginia (Loudoun County and adjacent counties), Dallas/Fort Worth, and Atlanta — all happen to have:
1. High lightning risk (Mid-Atlantic, Southeast)
2. High tornado risk (Texas, Georgia)
3. High hurricane risk (Gulf Coast adjacent)

But operators chose these locations because of fiber infrastructure, electricity costs, and proximity to corporate headquarters — not because of the hazard environment. The model cannot separate "located in Virginia" from "has high lightning risk" because we deliberately excluded `state` as a feature to avoid it absorbing structural feature variance.

**The drop in Stage 2**: In Stage 2, `hurricane_risk` drops from rank 6 to rank 29. `cold_wave_risk` drops from rank 11 to rank 30. `heat_wave_risk` drops from rank 8 to rank 20. This is the key diagnostic. Once you condition on "this county already has DCs" (Stage 2 training set), the hazard risk does not further predict how many DCs the county has. If the hazard relationship were causal, it should persist in Stage 2 — but it doesn't.

**Practical implication**: Do not read positive SHAP for hazard indices as "more risk → more DCs → build in risky areas." Read it as "the existing DC market happens to be in geographically risky areas." For normative siting recommendations, hazard indices should be treated as risk *costs* to be minimized, not as attractiveness *signals* to be chased.

### 12.5 The agglomeration signal

`wage_information` rising from Stage 1 rank 4 to Stage 2 rank 1 is the clearest structural finding in the analysis.

In Stage 1, IT wages play a moderate role — a county needs some IT labor market to attract its first data center, but other factors (power infrastructure, connectivity, economic density) dominate.

In Stage 2, IT wages become the #1 predictor of scale. Among counties that already have data centers, the depth of the IT labor market is the single strongest predictor of how large the cluster grows. This is **agglomeration**: once a DC cluster exists, it attracts IT talent; that talent concentration makes the county more attractive to the next operator; the next operator attracts more talent; and so on. The IT wage signal captures this self-reinforcing dynamic.

This also reflects **operational economics**: large multi-facility DC campuses can hire locally for operations, security, networking, and engineering roles. Counties with deeper IT labor markets reduce staffing costs and recruitment lead times for these roles.

### 12.6 Electricity price at scale

`commercial_price` rising from Stage 1 rank 9 to Stage 2 rank 4 reflects the role of electricity economics in cluster formation.

Electricity is typically 40–60% of a data center's total operating expense. At the presence stage, electricity cost plays a secondary role — you need a minimum threshold of power reliability and capacity first. But once a county has data centers, the marginal decision of whether to build the next facility (or to bring the next operator) is heavily influenced by price competitiveness.

The positive signed SHAP for commercial price in Stage 2 (higher price → more scale) seems counterintuitive. This is likely another geographic confound: the highest-scale counties (Loudoun VA, Maricopa AZ, Cook IL) are in regions with above-average utility prices. The model captures the co-occurrence, not the causal direction.

The `industrial_price` feature (Stage 2 rank 6) is more precisely the relevant price for large industrial power consumers, and its appearance in Stage 2 reinforces the electricity cost theme.

### 12.7 What broadband does NOT explain

`fiber_availability` and `any_tech_1000_100_coverage` both rank outside the top 20 for presence and scale. This is a genuine negative finding.

At the county level, broadband coverage rates are near-universal. Even rural counties show high coverage rates for at least basic broadband, and fiber coverage at 1 Gbps is broadly available in most large counties. What discriminates data center locations is not county-level coverage rates but specific physical infrastructure: the presence of major carrier-neutral fiber interconnect facilities (e.g., Equinix, CoreSite), dark fiber routes, and specific Points of Presence for hyperscale networks. These are sub-county features not captured in the FCC coverage statistics.

---

## 13. County Attractiveness Ranking

### 13.1 Score construction

All 3,138 counties are scored using the saved hurdle model:

```python
prob_all  = stage1.predict_proba(X)[:, 1]    # P(has DC | X) for all counties
scale_all = stage2.predict(X)                # E[count | DC present, X] for all counties
score_all = prob_all * scale_all             # hurdle expected count
```

The score is the model's expected data center count under the hurdle formulation — higher scores indicate more structurally attractive counties.

Note: Stage 2 was trained on the 80% training split. Applying it to all counties means the training counties' scores include some in-sample fit, making their scores potentially over-optimistic relative to true out-of-sample performance. The test set performance metrics (Section 11) provide the unbiased estimate of generalization.

### 13.2 Top counties

| Rank | County | State | Current DCs | P(presence) | Exp. scale | Score |
|---|---|---|---|---|---|---|
| 1 | Loudoun County | Virginia | 304.0 | 0.9853 | 18.06 | 17.79 |
| 2 | Maricopa County | Arizona | 136.1 | 0.9983 | 16.80 | 16.77 |
| 3 | Cook County | Illinois | 110.3 | 0.9990 | 16.47 | 16.45 |
| 4 | Dallas County | Texas | 125.7 | 0.9982 | 16.47 | 16.44 |
| 5 | Harris County | Texas | 50.8 | 0.9986 | 16.35 | 16.32 |

The model is well-calibrated against the known market: the top-ranked counties are recognized as the dominant US data center markets (Northern Virginia, Phoenix, Chicago, Dallas, Houston). This serves as a face validity check — a model that ranked obscure rural counties as most attractive would need to be re-examined.

### 13.3 Zero-DC counties with high attractiveness scores

More analytically interesting are counties with high model scores but zero current data centers — these are potential emerging markets or under-served locations:

| Rank (all counties) | County | State | P(presence) | Score |
|---|---|---|---|---|
| 80 | New Hanover County | North Carolina | 0.954 | 8.75 |
| 96 | Mercer County | New Jersey | 0.992 | 8.18 |
| 108 | Sussex County | Delaware | 0.979 | 7.71 |
| 112 | Queens County | New York | 0.874 | 7.52 |
| 137 | Calvert County | Maryland | 0.802 | 6.57 |

These counties share characteristics: proximity to major metro DC markets (New Hanover to Charlotte/Raleigh, Mercer to NYC/Philadelphia, Sussex to the Delmarva data center corridor, Queens to Manhattan, Calvert to Washington DC). They have the structural prerequisites (grid capacity, IT labor, economic density) but may lack the specific trigger (a hyperscale anchor tenant, a fiber POI) that would catalyze initial DC investment.

### 13.4 Interpreting the ranking output

The file `data_revealed/04_tables/county_attractiveness_ranking.csv` contains all 3,138 counties with:
- `p_presence`: Stage 1 probability (P(has DC | structural features))
- `expected_scale`: Stage 2 prediction (E[count | DC present, features])
- `attractiveness`: combined hurdle score (P × E)

These three columns allow different types of analysis:
- **First-mover targeting**: rank on `p_presence` to find counties that are structurally ready for any DC activity
- **Scale market targeting**: rank on `expected_scale` among counties above a `p_presence` threshold
- **Overall attractiveness**: rank on `attractiveness` for a balanced view

---

## 14. Reproducing the Analysis

### 14.1 Environment setup

```bash
# Clone the repository
git clone https://github.com/RX67-RX67/data-center-siting-analysis.git
cd data-center-siting-analysis

# Create a conda environment (Python 3.11 recommended)
conda create -n dc-analysis python=3.11 -y
conda activate dc-analysis

# Install dependencies
pip install -r requirements.txt

# macOS only: LightGBM requires libomp
brew install libomp
```

### 14.2 Running the notebooks

The notebooks must be run in order. The raw data pipeline scripts (`scripts/`) are not required if you start from the preprocessed table:

```bash
# If starting from raw data: run scripts first
python scripts/00_build_county_fips_table.py
python scripts/00_build_county_table.py
# ... (see scripts/ directory for full pipeline)
python scripts/03_build_county_final_table.py
python scripts/03_clean_county_final_table.py

# Then run notebooks in order:
jupyter lab
```

Open and execute in sequence:
1. `notebooks/eda.ipynb` — exploratory analysis (optional, for understanding)
2. `notebooks/preprocessing.ipynb` — produces `04_tables/county_preprocessed.csv`
3. `notebooks/modeling_option1_tweedie.ipynb` — produces `models/option1_tweedie.joblib`
4. `notebooks/modeling_option2_hurdle.ipynb` — produces `models/option2_hurdle.joblib`
5. `notebooks/interpretation.ipynb` — produces `04_tables/county_attractiveness_ranking.csv`

### 14.3 Loading saved models

If the models are already available, you can load them directly without rerunning the modeling notebooks:

```python
import joblib
import pandas as pd
import numpy as np

# Load Option 2 (hurdle) — primary model
obj    = joblib.load('models/option2_hurdle.joblib')
stage1 = obj['stage1']        # LGBMClassifier
stage2 = obj['stage2']        # LGBMRegressor
best_p = obj['best_p_s2']     # 1.8
feats  = obj['feature_names'] # list of 37 feature names

# Load the preprocessed data
df = pd.read_csv('data_revealed/04_tables/county_preprocessed.csv')
X  = df[feats]

# Score all counties
prob  = stage1.predict_proba(X)[:, 1]
scale = stage2.predict(X)
score = prob * scale

# Load Option 1 (Tweedie) — comparison model
obj1    = joblib.load('models/option1_tweedie.joblib')
model1  = obj1['model']
best_p1 = obj1['best_p']   # 1.3
```

---

## 15. Key Takeaways

### What the analysis found

1. **Power infrastructure is the primary presence prerequisite.** `clean_energy_jobs` and `grid_infrastructure_jobs` are the top two presence predictors. A county without a meaningful energy sector (76.5% of counties are at the suppression floor for `epg_natural_gas`) is extremely unlikely to have data centers. The energy sector employment captures grid capacity, transmission infrastructure, and institutional power relationships — all prerequisites for large-scale DC operation.

2. **IT labor market agglomeration drives scale.** `wage_information` rises from rank 4 (presence) to rank 1 (scale). Once a county has data centers, the depth of its IT labor market is the single strongest predictor of cluster size. This confirms the agglomeration hypothesis: DC clusters self-reinforce by building local IT talent.

3. **Electricity price becomes a competitive differentiator at scale.** `commercial_price` rises from rank 9 (presence) to rank 4 (scale). Power cost (40–60% of DC operating expense) matters more for the decision to expand a cluster than for the initial siting decision.

4. **Hazard risk indices are geographic confounders.** The high importance of `lightning_risk`, `hurricane_risk`, and `tornado_risk` for presence — with positive signs — reflects that the existing DC market happens to be in geographically risky areas (Virginia, Texas, Georgia), not that operators prefer risk. These indices drop from the top 10 to ranks 20–30 in Stage 2, confirming confounding.

5. **Broadband coverage rates do not discriminate.** County-level broadband coverage is near-universal; the discriminating factor is sub-county physical fiber infrastructure not captured in FCC statistics.

6. **Policy signals follow, not lead, market activity.** `has_policy_signal` and `policy_direction_score` rank consistently low. Policy discourse about data centers is concentrated in counties that already have significant DC activity.

7. **High land values signal presence, not absence.** Higher land values predict more data centers (positive SHAP for presence). Land value here proxies economic density — urban and peri-urban counties with higher land values have better supporting infrastructure.

### What the analysis cannot resolve

- **Causation vs. correlation**: SHAP values are correlational. `wage_information` being the top scale predictor could mean IT wages attract DCs, or DCs create IT jobs, or both. The analysis cannot distinguish between these without exogenous variation (e.g., a natural experiment in data center policy).

- **Sub-county variation**: All features are at the county level. Within a county, specific parcel characteristics (proximity to fiber POIs, utility substation capacity, industrial zoning) drive siting decisions that the model cannot see.

- **Future vs. past**: The model is trained on the current distribution of data centers, which reflects historical market dynamics (low electricity prices in the 2010s, specific hyperscale supplier relationships, historical policy environments). Future siting may weight water availability, renewable energy supply, and AI workload co-location differently.

- **The geographic confounding problem**: Excluding `state` was the right decision for interpretive purposes, but it means the hazard risk indices absorbed some of the geographic variance. A future approach using spatial lag variables or county-level fixed effects could partially resolve this.
