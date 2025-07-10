*I will assume my team are all staff engineers.*

## Ownership

### Profit

- **Unit-level (Experiments)**  
  - Primary Owner: DE A  
  - Secondary Owner: DE Me

- **Aggregate Profit (Investors)**  
  - Primary Owner: DE Me  
  - Secondary Owner: DE A

### Growth

- **Daily Growth (Experiments)**  
  - Primary Owner: DE B  
  - Secondary Owner: DE C

- **Aggregate Growth (Investors)**  
  - Primary Owner: DE Me  
  - Secondary Owner: DE B

### Engagement

- **Aggregate Engagement (Investors)**  
  - Primary Owner: DE Me  
  - Secondary Owner: DE C

---

## On-Call Schedule

- Rotating schedule: one DE per week.  
- Coverage can be adjusted so that either the primary or secondary owner assumes responsibility.  
- Business questions should be directed to the primary owner.

---

## Runbooks

### Aggregate Profit (Investors)

**Common Issues:**

- Product B arrives late in the data lake, so its dependent models must finish before this pipeline can start.
- If a new tax is created in the `tax_model` and the transformation doesn't include it, an alert will be triggered and the pipeline will break. This is to prevent propagating incorrect values downstream.

**SLA**: Data must be ready by **6 AM UTC**.

---

### Aggregate Growth (Investors)

**Common Issues:**

- **Upstream dependencies:**
  - If the `sales_force` upstream pipeline is delayed, open a ticket with the responsible DE team to resolve it as quickly as possible.  
  - `sales_force` is the main data source for this pipeline.  
  - If the delay exceeds 2 hours, start a thread and tag the DA leadership team to inform them of the issue.

**SLA**: Data must be ready by **9 AM UTC**.

---

### Aggregate Engagement (Investors)

**Common Issues:**

- We have a test to detect outliers in `web_events` data.  
  - If a warning is triggered, update the `ddos_referrer` table.  
  - On the next run, the warning should stop, as the anomaly can now be classified and tracked.

- **Airflow with OOM (Out of Memory):**
  - Open a ticket with the DevOps team.  
  - Clear the task and retry in Airflow.  
    - If it runs successfully, the issue was likely due to cluster resource contention.  
    - If the error persists, wait for DevOps team feedback.  
    - (Retrying usually works.)

**SLA**: Data must be ready by **9 AM UTC**.
