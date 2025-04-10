# BigData 2025 Project Repository

![TartuLogo](../images/logo_ut_0.png)

Project [Big Data](https://courses.cs.ut.ee/2025/bdm/spring/Main/HomePage) is provided by [University of Tartu](https://courses.cs.ut.ee/).

---
# Project 3: Analysing Flight Interconnected Data

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
   - In Jupyter’s file browser, open `project1.ipynb`.
   - Execute the cells to load data from `data/`, enrich with boroughs, and run the queries.

**Done!** You’re now running the NYC Taxi Data analysis via Docker. 

## Detailed Report

A more detailed write-up (including figures, tables, and extended explanations)  
is available in our PDF report: [pdfs/report.pdf](./pdf/report.pdf)

Simply open or download `pdfs/report.pdf` to read the full documentation.  

