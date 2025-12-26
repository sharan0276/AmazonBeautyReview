# Transparent Multi-Modal Product Success Prediction (Amazon Beauty) - WIP

Project: **Transparent Multi-Modal Product Success Prediction: An Explainable AI Approach to E-Commerce Review Analytics**  
Status: **Work in Progress (WIP)**

---

## 1) Overview
E-commerce teams need to spot **future winners early** for inventory planning, promotions, and recommendations. Most approaches use a single signal (ratings or text) and behave like a black box. This project builds a **multi-modal** predictor that combines:
- review text sentiment and language signals
- rating patterns (level + variance)
- helpfulness / engagement signals
- temporal dynamics (review velocity, trend shifts)

Goal: **higher accuracy + clear explanations** for business stakeholders.

---

## 2) Key Questions
- Can multi-modal features outperform text-only, rating-only, or time-only baselines?
- Which signals matter most for early success prediction?
- Can we explain each prediction in plain language (feature attributions)?

---

## 3) Data
Dataset: **Amazon Reviews 2023** (Beauty category)  
Inputs (examples):
- Ratings (1–5), rating distribution over time
- Review text (sentiment, topics, polarity strength)
- Helpfulness / votes (if available)
- Time signals (first N days/weeks, review velocity, rating stability)

Note: Exact schema depends on the extracted subset used in the pipeline.

---

## 4) Label Definition (WIP)
"Product success" can be defined in different ways. Final definition will be chosen based on available fields:
- **Classification**: Successful vs Not Successful (e.g., sustained high rating + strong review velocity)
- **Regression**: Predict future success score (e.g., next-28-day performance)

Leakage-safe rule: labels use **future window**, features use **early window** only (example: first 28 days).

---

## 5) Modeling Plan
### Baselines (Uni-Modal)
- Rating-only model (distributions, variance, trend)
- Text-only model (TF-IDF or embeddings + classifier)
- Time-only model (review velocity / trend features)

### Multi-Modal Model (Main)
- Early-window feature table + model (XGBoost / LightGBM / Logistic Regression)
- Optional stacking: combine text-model outputs with structured features

---

## 6) Evaluation
Primary metrics depend on label type:
- Classification: Macro F1, Precision/Recall, PR-AUC (focus on rare winners if imbalanced)
- Regression: MAE/RMSE, rank correlation (if used for prioritization)

Validation strategy:
- **Time-based split** (recommended) to match real deployment
- A/B contribution test: measure lift from adding each modality

---

## 7) Deliverables
- Trained multi-modal model with reproducible pipeline
- Explainability report (global + per-example)
- Stakeholder-facing summary (what signals matter and why)
- Optional: Streamlit dashboard for interactive analysis

---

