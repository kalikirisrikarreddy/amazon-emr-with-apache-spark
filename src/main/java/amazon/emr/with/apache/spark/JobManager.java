package amazon.emr.with.apache.spark;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.ActionOnFailure;
import software.amazon.awssdk.services.emr.model.AddJobFlowStepsRequest;
import software.amazon.awssdk.services.emr.model.Application;
import software.amazon.awssdk.services.emr.model.HadoopJarStepConfig;
import software.amazon.awssdk.services.emr.model.JobFlowInstancesConfig;
import software.amazon.awssdk.services.emr.model.PlacementType;
import software.amazon.awssdk.services.emr.model.RunJobFlowRequest;
import software.amazon.awssdk.services.emr.model.RunJobFlowResponse;
import software.amazon.awssdk.services.emr.model.StepConfig;

public class JobManager {

	static EmrClient emrClient = EmrClient.builder().region(Region.US_EAST_1)
			.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(null, null))).build();

	public static void main(String[] args) {
		createClusterAndRunSparkJob();
	}

	private static void createClusterAndRunSparkJob() {
		Application spark = Application.builder().name("Spark").build();
		Application hadoop = Application.builder().name("Hadoop").build();
		Application zeppelin = Application.builder().name("Zeppelin").build();

		StepConfig downloadingJarFromS3 = StepConfig.builder().name("Download Jar From S3 To EMR Cluster")
				.actionOnFailure(ActionOnFailure.TERMINATE_CLUSTER)
				.hadoopJarStep(HadoopJarStepConfig.builder().jar("command-runner.jar")
						.args("aws", "s3api", "get-object", "--bucket", "apache-spark-sample-code-artifacts-bucket-1",
								"--key", "apache-spark-sample-1.0.0-jar-with-dependencies.jar",
								"/home/hadoop/apache-spark-sample-1.0.0-jar-with-dependencies.jar")
						.build())
				.build();

		StepConfig workCountInSpark = StepConfig.builder().name("Spark Word Count")
				.actionOnFailure(ActionOnFailure.TERMINATE_CLUSTER)
				.hadoopJarStep(HadoopJarStepConfig.builder().jar("command-runner.jar")
						.args("spark-submit", "--deploy-mode", "client",
								"file:///home/hadoop/apache-spark-sample-1.0.0-jar-with-dependencies.jar")
						.build())
				.build();

		RunJobFlowRequest request = RunJobFlowRequest.builder().name("My EMR Cluster").releaseLabel("emr-6.12.0")
				.applications(spark, hadoop, zeppelin).logUri("s3://emr-with-apache-spark-logs-1")
				.serviceRole("EMR_DefaultRole").jobFlowRole("EMR_EC2_DefaultRole")
				.instances(JobFlowInstancesConfig.builder().ec2KeyName("kd1kp").instanceCount(1)
						.placement(PlacementType.builder().availabilityZones("us-east-1a").build())
						.keepJobFlowAliveWhenNoSteps(true).masterInstanceType("m4.large").build())
				.build();

		RunJobFlowResponse runJobFlowResponse = emrClient.runJobFlow(request);

		emrClient.addJobFlowSteps(AddJobFlowStepsRequest.builder().jobFlowId(runJobFlowResponse.jobFlowId())
				.steps(downloadingJarFromS3).build());
		emrClient.addJobFlowSteps(AddJobFlowStepsRequest.builder().jobFlowId(runJobFlowResponse.jobFlowId())
				.steps(workCountInSpark).build());
	}

}
