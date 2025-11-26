# Assignment Notebooks

This repository contains the notebooks used for **Question 1 (Q1)** and **Question 2 (Q2)**.  
All notebooks are Jupyter `.ipynb` files and were developed in **Google Colab**.

- **Q1:** Python for cohort extraction → R for the main analysis  
- **Q2:** Python for cohort extraction → R for descriptive statistics → Python for classification models  

---

## 1. File Overview

### `Q1_Cohort_extraction.ipynb`
- **Language:** Python  
- **Role:** Cohort extraction and data cleaning for Q1  
- **Main tasks:**
  - Load raw data files for Q1  
  - Apply inclusion and exclusion criteria  
  - Create and clean variables needed for analysis  
  - Save the cleaned Q1 cohort as a CSV (e.g. `Q1_cohort.csv`)  

### `Q1_Analysis_task.ipynb`
- **Language:** R  
- **Role:** Main analysis for Q1  
- **Main tasks:**
  - Load the cleaned Q1 cohort CSV from `Q1_Cohort_extraction.ipynb`  
  - Perform descriptive summaries and exploratory analysis  
  - Fit the primary statistical model(s) required for Q1  
  - Generate tables and figures for the report  

---

### `Q2_Cohort_extraction.ipynb`
- **Language:** Python  
- **Role:** Cohort extraction and data cleaning for Q2  
- **Main tasks:**
  - Load raw data files for Q2  
  - Apply Q2-specific inclusion and exclusion criteria  
  - Construct predictor and outcome variables  
  - Save the cleaned Q2 cohort as a CSV (e.g. `Q2_cohort.csv`)  

### `Q2_Descriptive_statistical_analysis.ipynb`
- **Language:** R  
- **Role:** Descriptive statistical analysis for Q2  
- **Main tasks:**
  - Load the Q2 cohort CSV created by `Q2_Cohort_extraction.ipynb`  
  - Summarise continuous variables as **mean (standard deviation)**  
  - Summarise categorical variables as **count (percentage)**  
  - Output formatted descriptive tables for the manuscript/report  

### `Q2_Classification_task.ipynb`
- **Language:** Python  
- **Role:** Main classification analysis for Q2  
- **Main tasks:**
  - Load the cleaned Q2 cohort CSV (and any additional processed features)  
  - Split data into training / validation / test sets  
  - Train and tune the classification model(s) for Q2  
  - Evaluate model performance with appropriate metrics and plots  
  - Save model outputs, performance summaries and figures  

---

## 2. Recommended Execution Order

### Q1 Workflow
1. **Run `Q1_Cohort_extraction.ipynb`** (Python)  
   - Edit input file paths at the top if needed  
   - Run all cells to generate `Q1_cohort.csv`  

2. **Run `Q1_Analysis_task.ipynb`** (R)  
   - Point to the `Q1_cohort.csv` file from step 1  
   - Run all cells to reproduce Q1 analyses, tables and figures  

---

### Q2 Workflow
1. **Run `Q2_Cohort_extraction.ipynb`** (Python)  
   - Edit input/output paths if necessary  
   - Run all cells to generate `Q2_cohort.csv`  

2. **Run `Q2_Descriptive_statistical_analysis.ipynb`** (R)  
   - Load `Q2_cohort.csv`  
   - Run all cells to obtain descriptive statistics tables  

3. **Run `Q2_Classification_task.ipynb`** (Python)  
   - Load `Q2_cohort.csv` (and any feature files if specified)  
   - Run all cells to fit the models and produce classification results  

---

## 3. Software and Dependencies

### Python
- **Version:** Python 3.x  
- **Environment:** Google Colab or local Jupyter  
- **Typical packages (see imports in each notebook):**
  - `pandas`, `numpy`
  - `scikit-learn`
  - `matplotlib` / `seaborn`

### R
- **Version:** R ≥ 4.0 (recommended)  
- **Environment:**  
  - R kernel in Jupyter/Colab, or  
  - RStudio / R console  
- **Typical packages (see imports in each R notebook):**
  - `tidyverse` (data wrangling and plotting)
  - Table/summary packages (e.g. `tableone`, `broom`, etc.)

---

## 4. Reproducibility Notes

- **File paths** for raw data and output CSVs are defined near the top of each notebook; update these if your folder structure is different.  
- **Random seeds** are set where relevant (e.g. train–test split, model training) to improve reproducibility.  
- Key intermediate datasets (such as `Q1_cohort.csv` and `Q2_cohort.csv`) are saved explicitly so that cohort extraction and analysis steps are clearly separated.

If you encounter errors:
1. Check that all required Python and R packages are installed.  
2. Verify that the input/output paths in the first cells of each notebook point to the correct locations on your system.
