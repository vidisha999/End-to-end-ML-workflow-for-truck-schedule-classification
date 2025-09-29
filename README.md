## 

## Project Overview and  Architecture 


I.  *Cloud-Native Data Foundation:*

  PostgreSQL and MySQL databases were deployed on AWS RDS to establish a robust and scalable data infrastructure. This setup enables structured data storage, fast SQL-based querying, and seamless integration with other AWS services and downstream analytics workflows. This supports real-time insights on the incoming streaming data.

II. *Exploratory Data Analysis (EDA):*

  EDA was performed using AWS SageMaker notebooks, connected directly to the RDS-hosted databases. This phase involved thorough data exploration, cleaning, and validation. The refined dataset was then stored in **Hopsworks Feature Store** for centralized access and reuse.

III. *Feature Engineering :*
  
  Cleaned data was retrieved from Hopsworks to perform preprocessing and feature engineering .Multiple data sources were merged to construct a final feature-rich DataFrame capturing key factors influencing truck delays. The engineered features were saved back to the Feature Store to ensure consistency and accessibility.

IV. *Model-Ready Dataset Preparation :*

  The final dataset was retrieved from Hopsworks and split into training, validation, and test sets. Further preprocessing included one-hot encoding of categorical variables and scaling of numerical features to prepare the data for machine learning models.

V. *Tier 5: Experiment Tracking with Weights & Biases (W&B):*

  A W&B project was initiated to manage model experimentation and performance tracking. Integration with the Python environment enabled seamless logging of metrics, artifacts, and hyperparameter configurations.

VI.  *Model Development and Selection :*

  Multiple models were built, trained and evaluated, followed by hyperparameter tuning uisng W&B sweeping techniques. The best-performing model was selected based on the highest performance metrics and consistency across data splits.

VII. *Application Deployment :*
   
  A Streamlit application was developed to serve predictions interactively. The final model was deployed on an AWS EC2 instance to ensure scalable and accessible inference.

VIII. *Monitoring and Automation:*

  Model monitoring was implemented to detect data and concept drift. CI/CD practices were integrated for automated deployment, and Amazon SageMaker Pipelines were used to orchestrate the end-to-end machine learning workflow. This ensures reliability, scalability, and operational efficiency in real-world logistics scenarios.








