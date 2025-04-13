# ✈️ Flight Interconnection Analysis Using Apache Spark and GraphFrames

**Authors**  
Juan Gonzalo Quiroz Cadavid, Priit Peterson, Shivam Maheshwari, Venkata Narayana Bommanaboina  
University of Tartu  
📧 juangonzalo@ut.ee | shivammahe21@gmail.com | bvnarayana515739@gmail.com | priit.petersonest@gmail.com  
📅 April 13, 2025

---

## 🚀 Overview

This project analyzes a large-scale flight dataset by modeling the US airport network as a graph. We use **Apache Spark** and **GraphFrames** to construct the graph and apply various graph algorithms to extract insights into airport connectivity, centrality, and robustness.

---

## 🛠 Graph Construction

The flight dataset was transformed into a directed graph with:

* **Vertices:** Unique airports extracted from both origin and destination fields  
* **Edges:** Directed flights between airports, annotated with metadata like flight date, distance, arrival time, and cancellation status

- **Vertices Count:** 296  
- **Edges Count:** ~2.7 million

---

## 📊 Queries & Analysis

### 📌 Query 1: Graph Statistics

We computed key metrics to understand the structure and connectivity of the airport network:

* **In-Degree:** Number of incoming flights per airport  
* **Out-Degree:** Number of outgoing flights  
* **Total Degree:** Combined connectivity of each airport  
* **Triangle Count:** Identified clusters via closed flight loops

These metrics helped identify major hubs and areas of dense interconnectivity.

---

### 🔺 Query 2: Total Triangle Count

* **Total triangles found:** 16,015  
* Indicates many alternate routing options and regional connectivity clusters in the network

---

### 🌐 Query 3: Eigenvector Centrality

Measured the influence of each airport based on the importance of its neighbors.

**Top Airports by Eigenvector Centrality:**
1. ATL  
2. ORD  
3. DFW  
4. DEN  
5. LAX  

This highlights airports that are not just busy, but also strategically positioned.

---

### 📈 Query 4: PageRank

Evaluated airport importance by simulating traveler behavior with random jumps.

**Top Airports by PageRank:**
1. ATL  
2. ORD  
3. DFW  
4. DEN  
5. LAX  

Similar to eigenvector centrality, but better at modeling real-world navigation and influence propagation.

---

### 🔗 Query 5: Connected Components

Analyzed the graph to detect isolated clusters or fully connected regions.

* **Result:** All 296 airports belong to one large connected component  
* Interpretation: Any airport is reachable from any other through a series of flights

---

## 🧰 Technologies Used

* Apache Spark
* GraphFrames
* Python
* Jupyter Notebook
---

## 📌 Key Takeaways

- Major hubs like ATL, ORD, and DFW are central to the network in both volume and influence  
- The US airport network is densely connected and highly resilient  
- Network analysis reveals strategic nodes crucial for robustness and route planning

---

## Detailed Report

A more detailed write-up (including figures, tables, and extended explanations)  
is available in our PDF report: [pdfs/report.pdf](./pdfs/report.pdf)

Simply open or download `pdfs/report.pdf` to read the full documentation.  
