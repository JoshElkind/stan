# StanQuant — Trading Algorithm Sandbox

StanQuant is a full-stack web platform for designing, uploading, and evaluating stock trading algorithms on millions of rows of historical data. 

Built to empower both beginner and experienced traders, this system provides a secure and scalable environment to validate the logic and performance of your strategies before risking real capital.

---

## Summary

As an undergraduate Computer Science student at the University of Waterloo, I created this project to solve a personal need — I wanted a place to test my own trading scripts using real, large-scale financial data with flexibility and safety.

This app lets users:
- Upload or create algorithmic trading strategies  
- Evaluate them using detailed statistical backtesting tools  
- Store, preview, and compare results for better decision-making  

---

## Tech Stack

- ⚛**Frontend:** React + Next.js + TailwindCSS  
- **Backend:** Django + Django REST Framework  
- **Data Processing:** Apache Spark (PySpark), Pandas, NumPy  
- **Database:** AWS RDS (PostgreSQL) with SQLAlchemy ORM  
- **Script Execution:** Kubernetes Jobs on AWS EKS with Dockerized workloads  
- **File Storage:** Amazon S3 (managed via `boto3`)  
- **Security:** All uploaded user files run inside isolated containers with timeouts, resource caps, and no external network access  

---

## Key Features

### Algorithm Upload & Validation
- Upload `.py` scripts containing a single strategy function (`def your_algo(df)`).
- Must return a `list` or `array` of Buy/Sell/Hold signals matching the length of the DataFrame.
- Uploaded files are validated before execution.

### Secure Execution Environment
- Each user-uploaded script is executed in a sealed Kubernetes container using AWS EKS.
- The job fetches historical stock data from AWS RDS, runs the algorithm, and stores output results back to S3.

### High-Performance Data Handling
- Evaluations run on minute-level historical stock data (millions of rows per test).
- Uses PySpark and Pandas for fast, scalable processing of time-series indicators, rolling windows, and deciders.

### Algorithm Evaluation System
- Users input metrics like:
  - Gain & Loss thresholds  
  - Intercept Ranges  
  - Clean conflict filtering  
  - Position duration  
- Algorithms can be combined and tested for consensus agreement.

---

## Demo Video

To access a comprehensive walkthrough of the platform, view the Loom video demo:

**[Demo Video Folder on Loom](https://loom.com/share/folder/42a2deaa3f5641a69579c3ad90bf5b70)**

---

## Photos

### Evaluate Your Algorithms ↓
![Evaluate Your Algorithms](./screenshots/13.png)
![Evaluate Your Algorithms](./screenshots/14.png)
![Evaluate Your Algorithms](./screenshots/15.png)
![Evaluate Your Algorithms](./screenshots/16.png)

### Evaluation Results ↓
![Evaluation Results](./screenshots/17.png)
![Evaluation Results](./screenshots/18.png)
![Evaluation Results](./screenshots/19.png)

### View Past Evaluations ↓
![View Past Evaluations](./screenshots/20.png)

### Evaluation Analytics ↓
![Evaluation Analytics](./screenshots/21.png)
![Evaluation Analytics](./screenshots/22.png)
![Evaluation Analytics](./screenshots/23.png)
![Evaluation Analytics](./screenshots/24.png)
![Evaluation Analytics](./screenshots/25.png)
![Evaluation Analytics](./screenshots/27.png)

#### View Your Algorithms ↓
![View Your Algorithms](./screenshots/3.png)

#### Preview Your Algorithms ↓
![Preview Your Algorithms](./screenshots/4.png)

#### Delete Your Algorithms ↓
![Delete Your Algorithms](./screenshots/5.png)

#### Upload Your Algorithms ↓
![Upload Your Algorithms](./screenshots/6.png)

#### Create Your Algorithms ↓
![Create Your Algorithms](./screenshots/7.png)
![Create Your Algorithms](./screenshots/8.png)
![Create Your Algorithms](./screenshots/9.png)
![Create Your Algorithms](./screenshots/10.png)

#### Homepage ↓
![Homepage](./screenshots/1.png)

#### Sign In ↓
![Sign In](./screenshots/12.png)

#### Sign Out ↓
![Sign Out](./screenshots/2.png)

#### About Page ↓
![About](./screenshots/28.png)

#### User Guide ↓
![User Guide](./screenshots/29.png)

---

## Links

- Website: [https://stanquant.com](https://stanquant.com)  

Note: Deployment currently paused, please contact me.
