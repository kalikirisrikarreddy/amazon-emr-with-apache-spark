# Execution steps

1. Clone this project onto your local machine.
2. Import the project into IDE(Eclipse, IntelliJ etc.) of your choice as a maven project.
3. Update the credentials at line number 25 of amazon.emr.with.apache.spark.JobManager.java to credentials of a user in your AWS account.
4. Edit the line numbers 39 and 54, to bucket names of your choice. Just make sure to create the buckets beforehand.
5. Run amazon.emr.with.apache.spark.JobManager to execute code which creates an EMR cluster, runs spark job on it and then terminates the cluster.
