# hockey_airflow
* About the Project ⬇️
* [Installation Guide](https://github.com/shawzy2/hockey_airflow/wiki/Installation-Guide)
* [Developer’s Guide](https://github.com/shawzy2/hockey_airflow/wiki/Developer's-Guide)
* [Future Direction](https://github.com/shawzy2/hockey_airflow/wiki/Future-Direction)

### Automate Data Scrapes from NHL's Public API
The NHL provides a [public API](https://gitlab.com/dword4/nhlapi) to fans for the purposes of experimental analysis. 
The API can be accessed via python requests library and used in hockey data science processes such as hypothesis testing, reporting analytics, and building machine learning models. 
While valuable on its own, the API can be coupled with software to automate daily web scrapes and seamlessly feed analytics dashboards and ML models the freshest data available.

One way to automate data ingestion is [Apache Airflow](https://airflow.apache.org/). 
Generally used for ETL pipelines, Airflow is a platform used to programmatically schedule and monitor workflows. 
This is the exact use case we need for updating datasets with the previous game’s data.


### Architecture
![hockey_airflow_design](https://user-images.githubusercontent.com/19720687/125180229-b071ff80-e1c5-11eb-8c6e-633432ba468e.png)

Above we see the architecture that will be used to implement automated data scrapes. To expand on each step:
* Step 1: Fetch list of gameId’s that occurred on previous day
  * Once a day at 4:30am est (a reasonable time to say that all games on the previous day have ended), the job will begin by getting a list of the previous day’s gameId’s.
* Step 2: Fetch game data for each gameId retrieved in step 1
  * Looping through the list of gameId’s retrieved in the preceding step, game event data will be retrieved for each gameId.
  * Steps 3 and 4 will execute for each gameId.
* Step 3: Group data by event type (shot, faceoff, etc.)
  * Within event data, there are many ‘event types’. For example, a few event types include:
    * Shots
    * Faceoffs
    * Penalties
    * Fights
    * Coaches challenges
  * It would be very beneficial to preemptively sort event types into bins for each ‘event type’. Compared to grouping by gameId, grouping by event type would allow for efficient data discovery and compatibility with high speed querying services (i.e. Amazon Athena).
* Step 4: Store data in S3

### Why Apache Airflow? Why not a simple Lambda function?
While the task of scheduling GET calls to an API, processing data, and delivering into S3 buckets can be achieved using a simple [Lambda](https://aws.amazon.com/lambda/) function, Airflow enables many valuable features down the road. 
For example, say we want to add an ML model that re-trained when new data was available. 
Well, since we’re using Airflow, we can schedule the model to pull data from S3 immediately after data delivery is finished.

Additionally, Airflow gives us resilience in scenarios where API calls fail and need to be re-tried. 
When processes upstream fail (i.e. step 1 in architecture section), they can be re-tried before proceeding to downstream processes (i.e. future ML model training).

### Final Product Screenshots
Here we see the end product of my work. On the left is a screenshot of Airflow's UI showing the order of tasks run in this pipeline. On the right is a preview of the format used to organize data within S3.
Airflow: Graph View of DAG |  S3: Goal Data per Game
:-------------------------:|:-------------------------:
![](https://user-images.githubusercontent.com/19720687/125184916-91886300-e1ef-11eb-8549-06f7b02a9095.png)  |  ![](https://user-images.githubusercontent.com/19720687/125184919-9220f980-e1ef-11eb-9dcb-42aede661fd3.png)
