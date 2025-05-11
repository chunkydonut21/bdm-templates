# BigData 2025 Project Repository

![TartuLogo](../images/logo_ut_0.png)

Project [Big Data](https://courses.cs.ut.ee/2025/bdm/spring/Main/HomePage) is provided by [University of Tartu](https://courses.cs.ut.ee/).

---
# Project 4: Airline Delay and Cancellation Prediction with Spark ML

**Students**  
- Juan Gonzalo Quiroz Cadavid  
- Priit Peterson  
- Shivam Maheshwari  
- Venkata Narayana Bommanaboina  

**Affiliation**: University of Tartu  

---


## Running the Project with Docker

1. **Ensure Docker is installed** (and Docker Compose if not included by default).
2. **Place** your CSV data and `nyc_boroughs.geojson` in the `./data` folder.
3. **Launch** the container:
   
       docker-compose up -d

   This starts a Jupyter + PySpark environment in the background.

4. **Check logs** (optional) to confirm Jupyter is running:
   
       docker-compose logs -f jupyter

5. **Open** your browser at [http://localhost:8888](http://localhost:8888).  
   - By default, no token/password is required (see `docker-compose.yml`).

6. **Run** the notebook/script:
   - In Jupyter’s file browser, open `project1.ipynb` (or your `.py` file).
   - Execute the cells to load data from `data/`, enrich with boroughs, and run the queries.

## Running the Project with Docker

1. **Ensure Docker is installed** (and Docker Compose if not included by default).
2. **Place** your CSV data in the `./data` folder.
3. **Launch** the container:
   
       docker-compose up -d

   This starts a Jupyter + PySpark environment in the background.

4. **Check logs** (optional) to confirm Jupyter is running:
   
       docker-compose logs -f jupyter

5. **Open** your browser at [http://localhost:8888](http://localhost:8888).  
   - By default, no token/password is required (see `docker-compose.yml`).

6. **Run** the notebook:
   - In Jupyter’s file browser, open `project_04.ipynb`.
   - Execute the cells to load data from `data/`, enrich with boroughs, and run the queries.

**Done!** You’re now running the NYC Taxi Data analysis via Docker. 


## 🎯 Task Breakdown & Grading
| Task                                   | Points | Description                                                               |
|----------------------------------------|:------:|----------------------------------------------------------------------------|
| **1. Data Ingestion & Preparation**    |   2    | Define schema, load 2009/2010 CSV, write Parquet partitioned by date.      |
| **2. Cleaning & Preprocessing**        |   2    | Rename fields, derive `DayOfWeek`/`Month`, drop null-heavy columns, filter diverted flights. |
| **3. Exploratory Analysis**            |   2    | Analyze top-10 carriers, cancellation codes, report imbalance ratio (72.7:1).|
| **4. Feature Engineering**             |   3    | Encode categoricals (Origin, Destination, CRSDepTime, DayOfWeek, Month) and assemble numeric features. |
| **5. Modeling & Hyperparameter Tuning**|   5    | Train LR, DT, RF, GBT; 3-fold CV on key hyperparameters; evaluate AUC + accuracy.|
| **6. Explainability**                  |   1    | Plot feature importances for the best tree-based model (Month & Destination top).|
| **7. Persistence & Inference**         |   2    | Save best model; evaluate on 2010 data (AUC=0.6403, Accuracy=0.9824).      |

---

## 📈 Key Results
- **Best 2009 model (CV)**: Logistic Regression achieved AUC=0.7428, Accuracy=0.9864.  
- **2010 evaluation**: AUC=0.6403, Accuracy=0.9824 (using persisted best model).  
- **Top features**: Month (Feb/Jun) and Destination drive cancellation predictions.

## Detailed Report

A more detailed write-up (including figures, tables, and extended explanations)  
is available in our PDF report: [pdfs/report.pdf](./pdfs/report.pdf)

Simply open or download `pdfs/report.pdf` to read the full documentation.  

