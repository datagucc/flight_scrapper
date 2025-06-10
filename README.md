# ✈️ Flight Price Tracker - Automated Data Pipeline

This project is a complete solution for extracting, processing, storing, and visualizing flight prices, built using a **Data Lakehouse architecture**. It enables monitoring flight prices for the upcoming year across multiple destinations and helps detect optimal fare opportunities.

## 🔍 Objectives

* Automate flight price collection from Google Flights via screenshots.
* Extract prices using OCR and centralize the data.
* Structure and enrich the data in a **Bronze ➝ Silver ➝ Gold** architecture.
* Visualize the data using **Power BI** dashboards.

---

## 📦 Project Structure

```bash
flight-price-tracker/
│
├── config/                 # Configuration files (paths, URLs, cities, etc.)
├── screenshots/            # Organized folders by date containing screenshots
├── ocr/                    # OCR scripts to extract data from screenshots
├── pipeline/               # Data transformation scripts (bronze, silver, gold)
├── metadata/               # Table tracking and data quality metrics
├── db/                     # Scripts to load data into PostgreSQL
├── visualisation/          # Power BI reports or dashboards
├── notebooks/              # EDA, tests and visual experiments
├── requirements.txt        # Project dependencies
└── README.md               # This file
```

---

## ⚙️ Features

### 1. 📸 Scraping & Screenshots (Playwright)

* Uses Playwright to automate Google Flights searches.
* Takes monthly price result screenshots.
* Destinations are configured in an Excel file.

### 2. 🛀 OCR & Extraction (Tesseract)

* Applies OCR to extract price data from screenshots.
* Stores raw data in a **Delta Lake table** with error logging.

### 3. 🛠️ Data Pipeline (Bronze ➝ Silver ➝ Gold)

* **Bronze**: Raw OCR data with possible errors.
* **Silver**: Cleaned, filtered, and typed data.
* **Gold**: Aggregated KPIs by route, date, and price trend.
* 100% Python implementation using Delta Lake (on-disk DeltaTables).

### 4. 📄 PostgreSQL Integration

* Gold-level data is exported to a **PostgreSQL server** for reporting.

### 5. 📊 Power BI Visualization

* Reads data directly from PostgreSQL.
* Dynamic reports with filters: destination, month, min/avg/max price, trends.

---

## 🔧 Technologies Used

| Area            | Tools/Technologies                                |
| --------------- | ------------------------------------------------- |
| Scraping & Bot  | [Playwright](https://playwright.dev/)             |
| OCR             | [Tesseract OCR](https://github.com/tesseract-ocr) |
| Raw Storage     | [Delta Lake](https://delta.io/) (local)           |
| Data Processing | Python (Pandas, PyDelta)                          |
| Database        | PostgreSQL                                        |
| Orchestration   | Airflow / Cron / Manual script                    |
| Visualization   | Power BI                                          |

---

## 🚀 Getting Started

### 1. Prerequisites

* Python ≥ 3.10
* [Playwright](https://playwright.dev/python/) installed and configured
* [Tesseract OCR](https://tesseract-ocr.github.io/) installed locally
* Local or remote PostgreSQL server
* Power BI Desktop (to open `.pbix` report)

### 2. Installation

```bash
git clone https://github.com/your-username/flight-price-tracker.git
cd flight-price-tracker
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install
```

### 3. Configuration

* Define routes in `config/routes.xlsx`
* Set paths in `config/config.yaml`

### 4. Manual Execution

```bash
# Take screenshots
python scripts/screenshot_bot.py

# Run OCR to extract prices
python ocr/parse_screenshots.py

# Run the transformation pipeline
python pipeline/run_pipeline.py

# Load Gold data to PostgreSQL
python db/load_to_postgres.py
```

---

## 📈 Power BI Report Samples



---

## 📌 Coming Soon

* Orchestration with Airflow or Prefect
* Email or Telegram alerts for best deals detected
* Cloud deployment (e.g., S3, Databricks)
* Web UI (Flask or Streamlit)

---

## 👨‍💻 Author

**\Augustin Nollevaux**
Expert Data 
[LinkedIn](https://ar.linkedin.com/in/a-nollevaux) · [GitHub](https://github.com/datagucc)

---


