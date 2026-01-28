# Transparent Multi-Modal Product Success Prediction (Amazon Beauty)

Project: **Transparent Multi-Modal Product Success Prediction: An Explainable AI Approach to E-Commerce Review Analytics**  

This project predicts early product success on Amazon using review data. Retail teams care about this because early signals help with inventory planning, ranking, and knowing which new products will gain traction. The goal is to build a simple, transparent model using three short-term signals: ratings, sentiment, and review velocity.

The project compares rating-only, text-only, time-only, and multimodal models. A/B-style ablation tests are used to see how much signal each modality and feature group contributes. Logistic Regression, XGBoost, and Histogram Gradient Boosting are evaluated using PR-AUC, Precision@K, Recall@K, and Lift@K. SHAP is used to explain feature impact and highlight which early signals matter most.

---
## Key Results

Multimodal features perform best. PR-AUC improves from about 0.48 (ratings only) to about 0.53 when all signals are combined. Logistic Regression is the strongest model and stays consistent across five random seeds (mean PR-AUC ≈ 0.525). It also achieves a Precision@100 of about 0.65 and a Lift@100 of around 2.6 over the 25% baseline.

A/B testing confirms the strongest signals come from:
- rating level and rating trend  
- review velocity  
- sentiment tone  

Helpfulness features were tested and removed because they added noise.

---

## Retail Impact

This system helps retail teams:
- monitor new product launches and identify potential winners early  
- detect weak launches before they become costly  
- improve recommendation and ranking relevance using early-review signals  
- support category managers with SHAP-backed explanations instead of black-box predictions  

The final model is simple, stable, and fully interpretable, making it practical for real retail workflows.

---

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

It is recommended to upload the processed data files (from `data/processed/stage1_output`) to Google Colab and run these notebooks.

---

## Future Work

Next steps to expand the project:  
1. Add real-time scoring for continuous product monitoring  
2. Build a dashboard (Power BI or Streamlit) for product health insights  
3. Add other retail signals like price, images, seller data, and traffic  
4. Add a rolling time window so the model updates its predictions as new reviews arrive, making the system dynamic.
---