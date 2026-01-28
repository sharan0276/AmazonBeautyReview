# Transparent Multi-Modal Product Success Prediction (Amazon Beauty) - WIP

Project: **Transparent Multi-Modal Product Success Prediction: An Explainable AI Approach to E-Commerce Review Analytics**  

## Project Overview

This project predicts early product success on Amazon using review data. Retail teams care about this because early signals help with inventory planning, ranking, and reacting faster to weak launches. I build a simple, explainable machine learning system that uses three types of review signals: ratings, sentiment, and review velocity.

The project compares rating-only, text-only, time-only, and multimodal models. Logistic Regression, XGBoost, and Histogram Gradient Boosting are tested. I evaluate everything using PR-AUC, Precision@K, and Lift@K, which are more meaningful for retail ranking. SHAP is used across the project to explain feature impact, show why predictions change, and highlight which signals matter most.

## Key Results

Multimodal features perform best. PR-AUC increases from about 0.48 (ratings only) to about 0.53 when combining ratings, sentiment, and time patterns. Logistic Regression is the strongest model, with a mean PR-AUC of about 0.525 across five seeds. It also delivers a Precision@100 of about 0.65 and a Lift@100 of about 2.6 over the 25 percent baseline.

SHAP analysis shows the most important early indicators:  
- rating level and rating trend  
- review velocity (reviews per day)  
- sentiment tone from recent reviews  

Helpfulness features were tested but removed because they added noise.

## Retail Impact

This system helps retail teams:  
- score new products early and identify potential winners  
- spot weak launches before they become costly  
- improve recommendation ranking with a clear, explainable model  
- support category managers with SHAP-backed insights instead of black-box outputs  

The model is simple, stable, and fully interpretable, which makes it practical for real retail workflows.

## How to Run the Data Pipeline
### 1. Download & Prepare Data
Run the main module to download the dataset and save the raw parquet files:
```bash
python3 -m src.main
```
This will create `data/processed/reviews_raw.parquet` and `data/processed/meta_raw.parquet`.

### 2. Run Stage 1 Curation
Run the Stage 1 build script to curate the dataset (filtering, eligibility, observability checks):
```bash
python3 -m src.build_stage1 --reviews data/processed/reviews_raw.parquet --meta data/processed/meta_raw.parquet --out data/processed/stage1_output --tags "initial_run"
```
**Note**: `src.build_stage1` must be run as a module (`-m`) due to relative imports and project structure.

### 3. Notebook Analysis
The modules executed above (`src`) implement the data preparation logic from **Notebook 1**.
For the remainder of the project, please proceed with **Notebook 2 (EDA & Feature Analysis)** and **Notebook 3 (Model Training)**.

It is recommended to upload the processed data files (from `data/processed/stage1_output`) to Google Colab and run these notebooks there for optimal performance and dependency management.

## Future Work

Next steps to expand the project:  
1. Add real-time scoring for continuous product monitoring  
2. Build a dashboard (Power BI or Streamlit) for product health insights  
3. Add other retail signals like price, images, seller data, and traffic  
4. Add a rolling time window so the model updates its predictions as new reviews arrive.  
   This makes the system dynamic instead of one-time, and matches how retail teams monitor live product health.
