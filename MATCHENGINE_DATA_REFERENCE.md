# MatchEngine Data Reference

## 1. Reference Data (Patients)

### Source
- **MongoDB database:** `integration` (tests) or your production db
- **Collection:** `clinical`
- **Loaded from:** `matchengine/tests/data/integration_data/clinical.bson.gz` (test data)

### Clinical Document Fields

| Field | Description |
|---|---|
| `SAMPLE_ID` | Primary patient/sample identifier (used as join key) |
| `MRN` | Medical record number |
| `GENDER` | Patient gender |
| `BIRTH_DATE` | Date of birth (datetime) |
| `BIRTH_DATE_INT` | Birth date as integer (YYYYMMDD - 1) — used for age range queries |
| `ONCOTREE_PRIMARY_DIAGNOSIS_NAME` | Cancer diagnosis using OncoTree terminology |
| `VITAL_STATUS` | `"alive"` or `"deceased"` |
| `REPORT_DATE` | Date of genomic report |
| `TUMOR_MUTATIONAL_BURDEN_PER_MEGABASE` | TMB value (used for TMB-based trial matching) |
| `MMR_STATUS` | Mismatch repair status |

### Genomic Document Fields
- **Collection:** `genomic`
- **Join field:** `CLINICAL_ID` → `clinical._id`
- **Loaded from:** `matchengine/tests/data/integration_data/genomic.bson.gz`

| Field | Description |
|---|---|
| `CLINICAL_ID` | Foreign key back to `clinical._id` |
| `SAMPLE_ID` | Sample identifier |
| `TRUE_HUGO_SYMBOL` | Gene name (e.g. `EGFR`, `KRAS`) |
| `TRUE_PROTEIN_CHANGE` | Protein-level change (e.g. `p.E746_A750del`) |
| `TRUE_CDNA_CHANGE` | cDNA-level change |
| `TRUE_VARIANT_CLASSIFICATION` | Variant class (e.g. `In_Frame_Del`, `Missense_Mutation`) |
| `VARIANT_CATEGORY` | High-level category: `MUTATION`, `CNV`, `SV` |
| `CNV_CALL` | Copy number call (e.g. `High level amplification`) |
| `TRUE_TRANSCRIPT_EXON` | Exon number |
| `WILDTYPE` | Boolean — whether this is a wildtype record |
| `ALLELE_FRACTION` | Variant allele fraction |
| `REFERENCE_ALLELE` | Reference allele |
| `TIER` | Variant tier (1–4) |
| `CHROMOSOME` | Chromosome |
| `POSITION` | Genomic position |
| `ACTIONABILITY` | Actionability annotation |
| `MMR_STATUS` | MMR/MSI status on the genomic record |
| `APOBEC_STATUS` | APOBEC mutational signature |
| `POLE_STATUS` | POLE mutational signature |
| `TABACCO_STATUS` | Tobacco mutational signature |
| `TEMOZOLOMIDE_STATUS` | Temozolomide mutational signature |
| `UVA_STATUS` | UV mutational signature |
| `FUSION_PARTNER_HUGO_SYMBOL` | Second gene in a fusion |
| `LEFT_PARTNER_GENE` / `RIGHT_PARTNER_GENE` | Structural variant partners |
| `STRUCTURAL_VARIANT_COMMENT` | Free-text SV description (used for unstructured SV matching) |
| `STRUCTURAL_VARIANT_TYPE` | SV type |
| `STRUCTURED_SV` | Boolean — whether SV has structured data |

---

## 2. Query Data (Clinical Trials)

### Source
- **MongoDB collection:** `trial`
- **Source files (test):** `matchengine/tests/data/integration_trials/*.json`
- **Trial identifier:** `protocol_no`

### Trial Document Structure (CTML)

Trials are written in **CTML (Clinical Trial Markup Language)** — a nested JSON schema.

```
trial
└── protocol_no          # e.g. "10-002"
└── status               # e.g. "open to accrual"
└── treatment_list
    └── step[]
        ├── step_code
        ├── step_internal_id
        ├── match[]          ← step-level eligibility criteria
        └── arm[]
            ├── arm_code
            ├── arm_internal_id
            ├── arm_suspended
            ├── match[]      ← arm-level eligibility criteria (most specific)
            └── dose_level[]
                ├── level_code
                ├── level_internal_id
                ├── level_suspended
                └── match[]  ← dose-level criteria
```

### CTML Match Block Structure

Each `match` block is a tree of `and`/`or` nodes containing `clinical` and `genomic` leaf criteria:

```json
{
  "and": [
    {
      "genomic": {
        "hugo_symbol": "EGFR",
        "variant_classification": "In_Frame_Del",
        "variant_category": "Mutation"
      }
    },
    {
      "clinical": {
        "age_numerical": ">=18",
        "oncotree_primary_diagnosis": "Non-Small Cell Lung Cancer"
      }
    }
  ]
}
```

### Integration Test Trials

| File | Protocol | Description |
|---|---|---|
| `all_open.json` | 10-002 | EGFR In_Frame_Del, NSCLC arm |
| `all_closed.json` | — | All arms/steps closed |
| `all_closed_trial_closed.json` | — | Entire trial closed |
| `tmb.json` | 10-003 | TMB-based matching |
| `signatures.json` | — | Mutational signature matching |
| `structured_sv.json` | — | Structured SV matching |
| `unstructured_sv.json` | — | Free-text SV comment matching |
| `wildcard_protein_found.json` | 10-007 | Wildcard protein change matching |
| `wildcard_protein_not_found.json` | — | Wildcard negative test |
| `older_than_18.json` | — | Age criteria test |
| `younger_than_18.json` | — | Age criteria test |
| `closed_dose.json` | — | Dose level closed |
| `closed_step_arm.json` | — | Step/arm closed |
| `run_log_*.json` | — | Run log / incremental update tests |

---

## 3. Matching Criteria Configuration

### Config File
**`matchengine/config/dfci_config.json`**

This file defines **how CTML trial keys map to patient document fields** and is the central configuration for what gets matched and how.

### CTML → Patient Field Mappings

#### Clinical Criteria (`ctml_collection_mappings.clinical.trial_key_mappings`)

| CTML Key | Patient Field | Transform |
|---|---|---|
| `AGE_NUMERICAL` | `BIRTH_DATE_INT` | `age_range_to_date_int_query` — converts age expression (e.g. `>=18`) to a date integer range |
| `ONCOTREE_PRIMARY_DIAGNOSIS` | `ONCOTREE_PRIMARY_DIAGNOSIS_NAME` | `external_file_mapping` via `matchengine/ref/oncotree_mapping.json` — expands a diagnosis name to all matching OncoTree subtypes |
| `GENDER` | `GENDER` | Direct match (`nomap`) |
| `TMB_NUMERICAL` | `TUMOR_MUTATIONAL_BURDEN_PER_MEGABASE` | `tmb_range_to_query` — converts expression to numeric range query |
| `HER2_STATUS` | *(ignored)* | — |
| `PR_STATUS` | *(ignored)* | — |
| `ER_STATUS` | *(ignored)* | — |
| `DISEASE_STATUS` | *(ignored)* | — |

#### Genomic Criteria (`ctml_collection_mappings.genomic.trial_key_mappings`)

| CTML Key | Patient Field | Transform |
|---|---|---|
| `HUGO_SYMBOL` | `TRUE_HUGO_SYMBOL` | Direct match |
| `EXON` | `TRUE_TRANSCRIPT_EXON` | Direct match |
| `PROTEIN_CHANGE` | `TRUE_PROTEIN_CHANGE` | Direct match |
| `WILDCARD_PROTEIN_CHANGE` | `TRUE_PROTEIN_CHANGE` | `wildcard_regex` — supports `p.V600?` style patterns |
| `VARIANT_CLASSIFICATION` | `TRUE_VARIANT_CLASSIFICATION` | Direct match |
| `VARIANT_CATEGORY` | `VARIANT_CATEGORY` | `variant_category_map` — maps CTML values to patient vocab |
| `CNV_CALL` | `CNV_CALL` | `cnv_map` |
| `WILDTYPE` | `WILDTYPE` | Direct match |
| `MMR_STATUS` | `MMR_STATUS` | `mmr_ms_map` |
| `MS_STATUS` | `MMR_STATUS` | `mmr_ms_map` |
| `APOBEC_SIGNATURE` | `APOBEC_STATUS` | Direct match |
| `POLE_SIGNATURE` | `POLE_STATUS` | Direct match |
| `TOBACCO_SIGNATURE` | `TABACCO_STATUS` | Direct match |
| `TEMOZOLOMIDE_SIGNATURE` | `TEMOZOLOMIDE_STATUS` | Direct match |
| `UVA_SIGNATURE` | `UVA_STATUS` | Direct match |
| `FUSION_PARTNER_HUGO_SYMBOL` | `FUSION_PARTNER_HUGO_SYMBOL` | Direct match |
| `DISPLAY_NAME` | *(ignored)* | — |

### Special Matching Logic (plugins)

Beyond the config, DFCI-specific plugins apply additional transforms:

- **`DFCIQueryNodeTransformer.py`** — rewrites SV queries: if a trial has `STRUCTURAL_VARIANT_COMMENT` but no structured SV partner gene, it converts to a regex search of the free-text comment field.
- **`DFCIMatchCriteriaTranform.py`** — handles age date-integer conversion, OncoTree hierarchy expansion, TMB range queries, and variant category / CNV / MMR value mapping.
- **`DFCITrialMatchDocumentCreator.py`** — post-processes results: if any match reason for a patient is `show_in_ui=False`, all reasons for that patient are flipped to `False` (all-or-nothing per patient).

---

## 4. Match Output — `trial_match` Collection Fields

Each document represents one **patient × trial arm × genomic alteration** match.

### Patient Identity

| Field | Description |
|---|---|
| `sample_id` | Patient sample ID (from `clinical.SAMPLE_ID`) |
| `mrn` | Patient MRN |
| `clinical_id` | MongoDB `_id` of the matching `clinical` document |
| `genomic_id` | MongoDB `_id` of the matching `genomic` document |

### Patient Clinical Data (denormalized from `clinical`)

| Field | Description |
|---|---|
| `gender` | Patient gender |
| `vital_status` | `"alive"` or `"deceased"` |
| `oncotree_primary_diagnosis_name` | Patient's cancer diagnosis |
| `report_date` | Genomic report date |

### Genomic Alteration (denormalized from `genomic`)

| Field | Description |
|---|---|
| `genomic_alteration` | Human-readable alteration string, e.g. `"EGFR p.E746_A750del"` |
| `true_hugo_symbol` | Gene name |
| `true_protein_change` | Protein change |
| `true_cdna_change` | cDNA change |
| `true_variant_classification` | Variant classification |
| `true_transcript_exon` | Exon number |
| `variant_category` | `MUTATION`, `CNV`, or `SV` |
| `cnv_call` | CNV call if applicable |
| `wildtype` | Boolean |
| `allele_fraction` | Variant allele fraction |
| `reference_allele` | Reference allele |
| `tier` | Variant tier (1–4; 0 = untiered) |
| `chromosome` | Chromosome |
| `position` | Genomic position |
| `actionability` | Actionability annotation |
| `mmr_status` | MMR/MSI status |

### Trial Identity

| Field | Description |
|---|---|
| `protocol_no` | Trial protocol number |
| `internal_id` | Internal ID of the matched arm/step/dose level |
| `code` | Human-readable arm/step code (e.g. `"ARM 1"`) |

### Match Mechanics

| Field | Description |
|---|---|
| `match_level` | Where in the CTML hierarchy the match occurred: `"arm"`, `"step"`, or `"dose"` |
| `match_path` | Dot-path to the CTML node that matched, e.g. `"treatment_list.step.0.arm.0.match"` |
| `match_type` | What genomic feature drove the match: `"variant"` (exact protein change), `"gene"` (gene-level), `"mmr"`, `"tmb"`, etc. |
| `reason_type` | `"genomic"` or `"clinical"` — which collection provided the match reason |
| `cancer_type_match` | `"specific"`, `"all_solid"`, or `"all_liquid"` — how specific the oncotree diagnosis match was |
| `combo_coord` | SHA1 hash identifying the specific combination of criteria nodes that matched (the path through the match tree) |
| `query_hash` | Hash of the raw MongoDB query that produced this match |

### Trial Status

| Field | Description |
|---|---|
| `trial_curation_level_status` | Status of the matched arm/dose level: `"open"` or `"closed"` |
| `trial_summary_status` | Overall trial status: `"open"` or `"closed"` |
| `coordinating_center` | Trial coordinating center (e.g. `"Dana-Farber Cancer Institute"`) |
| `is_disabled` | Boolean — suppressed from results |

### UI / Display

| Field | Description |
|---|---|
| `show_in_ui` | Boolean — whether this match should be surfaced in the MatchMiner UI. Set to `False` if deceased/closed; all-or-nothing per patient (see `DFCITrialMatchDocumentCreator`) |
| `sort_order` | Array of integers used to sort matches for display, derived from `trial_match_sorting` in `dfci_config.json`. Lower = higher priority. Factors: `show_in_ui`, `trial_curation_level_status`, `match_type`, `tier`, `cnv_call`, `wildtype`, `cancer_type_match`, `coordinating_center`, `protocol_no` |

### Internal / Housekeeping

| Field | Description |
|---|---|
| `hash` | SHA1 of the full match document — used for deduplication and incremental updates |
| `_me_id` | MatchEngine run ID that produced this document |
| `_updated` | Timestamp of last upsert |
| `q_depth` | Query tree depth of the match node |
| `q_width` | Number of genomic documents that contributed to this match |

---

## 5. Quick Reference: Data Flow

```
dfci_config.json              oncotree_mapping.json
     |                               |
     v                               v
CTML trial.match[] ──── DFCIMatchCriteriaTranform ───► MongoDB queries
                                                            |
                                                    clinical collection
                                                    genomic collection
                                                            |
                                                            v
                                               DFCIQueryNodeTransformer
                                               (SV rewrite, subsetting)
                                                            |
                                                            v
                                              DFCITrialMatchDocumentCreator
                                              (show_in_ui, denormalization)
                                                            |
                                                            v
                                                   trial_match collection
```

---

## 6. Worked Example: Patient × Trial Match

**Patient:** MRN `1035` — EGFR p.E746_A750del → Trial `10-002` ARM 1

### Step 1 — The patient's documents

**`clinical._id = ObjectId("5d2799d86756630d8dd065b8")`**
```json
{
  "SAMPLE_ID":                       "5d2799d86756630d8dd065b8",
  "MRN":                             "1035",
  "GENDER":                          "Male",
  "VITAL_STATUS":                    "alive",
  "BIRTH_DATE":                      "1948-09-02",
  "BIRTH_DATE_INT":                  19480901.0,
  "ONCOTREE_PRIMARY_DIAGNOSIS_NAME": "Non-Small Cell Lung Cancer"
}
```

**`genomic._id = ObjectId("5d279d556756630d8dda10f1")`**
```json
{
  "CLINICAL_ID":              "5d2799d86756630d8dd065b8",   ← joins to clinical._id
  "TRUE_HUGO_SYMBOL":         "EGFR",
  "TRUE_PROTEIN_CHANGE":      "p.E746_A750del",
  "TRUE_VARIANT_CLASSIFICATION": "In_Frame_Del",
  "VARIANT_CATEGORY":         "MUTATION",
  "TRUE_TRANSCRIPT_EXON":     19,
  "WILDTYPE":                 false,
  "ALLELE_FRACTION":          0.0,
  "TIER":                     0
}
```

### Step 2 — The trial's CTML criteria (ARM 1 of `10-002`)

```
treatment_list.step[0].arm[0].match  (match_path in output)
│
└── AND
    ├── genomic:
    │     hugo_symbol:             "EGFR"          → maps to TRUE_HUGO_SYMBOL
    │     variant_classification:  "In_Frame_Del"  → maps to TRUE_VARIANT_CLASSIFICATION
    │     variant_category:        "Mutation"      → maps to VARIANT_CATEGORY (via variant_category_map)
    │
    └── clinical:
          age_numerical:              ">=18"        → maps to BIRTH_DATE_INT (age_range_to_date_int_query)
          oncotree_primary_diagnosis: "Non-Small Cell Lung Cancer"
                                                   → maps to ONCOTREE_PRIMARY_DIAGNOSIS_NAME (via oncotree_mapping.json)
```

All key→field translations are defined in `dfci_config.json → ctml_collection_mappings`.

### Step 3 — How each criterion is evaluated

| CTML criterion | Patient value | Pass? | Notes |
|---|---|---|---|
| `hugo_symbol = "EGFR"` | `TRUE_HUGO_SYMBOL = "EGFR"` | ✓ | Direct string match |
| `variant_classification = "In_Frame_Del"` | `TRUE_VARIANT_CLASSIFICATION = "In_Frame_Del"` | ✓ | Direct string match |
| `variant_category = "Mutation"` | `VARIANT_CATEGORY = "MUTATION"` | ✓ | `variant_category_map` normalizes case |
| `age_numerical >= 18` | `BIRTH_DATE_INT = 19480901` → age ~77 | ✓ | Converted to date integer range query |
| `oncotree_primary_diagnosis = "Non-Small Cell Lung Cancer"` | `ONCOTREE_PRIMARY_DIAGNOSIS_NAME = "Non-Small Cell Lung Cancer"` | ✓ | `oncotree_mapping.json` expands to all subtypes; exact match here |

Because this is an `AND` block, **all five must pass** — they do.

Note: the match hits at the **gene level** (hugo_symbol + variant_classification), not the **variant level** (exact protein change). The trial doesn't specify `protein_change`, so any EGFR In_Frame_Del qualifies. This is why `match_type = "gene"` in the output, not `"variant"`.

### Step 4 — The resulting `trial_match` document

```json
{
  "sample_id":    "5d2799d86756630d8dd065b8",   ← from clinical.SAMPLE_ID
  "clinical_id":  "5d2799d86756630d8dd065b8",   ← clinical._id
  "genomic_id":   "5d279d556756630d8dda10f1",   ← genomic._id of the matching alteration
  "mrn":          "1035",
  "protocol_no":  "10-002",                      ← links back to trial.protocol_no
  "internal_id":  211,                           ← arm_internal_id from CTML
  "code":         "ARM 1",

  "match_level":  "arm",                         ← which CTML level matched
  "match_path":   "treatment_list.step.0.arm.0.match",
  "match_type":   "gene",                        ← gene-level, not exact protein change
  "reason_type":  "genomic",                     ← genomic doc drove the match
  "cancer_type_match": "specific",               ← diagnosis matched a specific OncoTree node

  "genomic_alteration":        "EGFR p.E746_A750del",
  "true_hugo_symbol":          "EGFR",
  "true_protein_change":       "p.E746_A750del",
  "true_variant_classification": "In_Frame_Del",
  "variant_category":          "MUTATION",
  "true_transcript_exon":      19,
  "wildtype":                  false,

  "oncotree_primary_diagnosis_name": "Non-Small Cell Lung Cancer",
  "gender":        "Male",
  "vital_status":  "alive",

  "trial_curation_level_status": "open",
  "trial_summary_status":        "open",
  "show_in_ui":    true,
  "sort_order":    [1, 99, 1, 99, 99, 99, 10002]
}
```

### Document relationships at a glance

```
clinical._id  ──────────────────────────────────► trial_match.clinical_id
clinical._id  ◄── genomic.CLINICAL_ID
genomic._id   ──────────────────────────────────► trial_match.genomic_id
trial.protocol_no ──────────────────────────────► trial_match.protocol_no
trial.treatment_list.step[0].arm[0].arm_internal_id → trial_match.internal_id
```
