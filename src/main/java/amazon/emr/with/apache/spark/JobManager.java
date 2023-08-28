package amazon.emr.with.apache.spark;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.ActionOnFailure;
import software.amazon.awssdk.services.emr.model.Application;
import software.amazon.awssdk.services.emr.model.HadoopJarStepConfig;
import software.amazon.awssdk.services.emr.model.JobFlowInstancesConfig;
import software.amazon.awssdk.services.emr.model.RunJobFlowRequest;
import software.amazon.awssdk.services.emr.model.StepConfig;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.iam.model.AttachRolePolicyRequest;
import software.amazon.awssdk.services.iam.model.CreateRoleRequest;

public class JobManager {

	static IamClient iamClient = IamClient.builder().region(Region.US_EAST_1)
			.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(null, null))).build();

	static EmrClient emrClient = EmrClient.builder().region(Region.US_EAST_1)
			.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(null, null))).build();

	public static void main(String[] args) {
		createNecessaryServiceRoles();
		Application spark = Application.builder().name("Spark").build();
		RunJobFlowRequest request = RunJobFlowRequest.builder().name("My EMR Cluster").releaseLabel("emr-6.0.0")
				.steps(StepConfig.builder().name("Spark Word Count").actionOnFailure(ActionOnFailure.TERMINATE_CLUSTER)
						.hadoopJarStep(HadoopJarStepConfig.builder()
								.jar("s3://apache-spark-sample-code-artifacts-bucket-1/apache-spark-sample.jar")
								.mainClass("apache.spark.SparkSample.java").build())
						.build())
				.applications(spark).logUri("s3://emr-with-apache-spark-logs-1").serviceRole("EMRRole")
				.jobFlowRole("EMREC2Role")
				.instances(JobFlowInstancesConfig.builder().ec2SubnetId("subnet-05b221f1672c794d7").ec2KeyName("kd1kp")
						.instanceCount(1).keepJobFlowAliveWhenNoSteps(false).masterInstanceType("m4.large").build())
				.build();
		emrClient.runJobFlow(request);
	}

	private static void createNecessaryServiceRoles() {
		// This role will be assumed by EMR service to access other AWS services on our
		// behalf.
		iamClient.createRole(CreateRoleRequest.builder().roleName("EMRRole").assumeRolePolicyDocument("""
				{
				    "Version": "2012-10-17",
				    "Statement": [
				        {
				            "Effect": "Allow",
				            "Action": [
				                "sts:AssumeRole"
				            ],
				            "Principal": {
				                "Service": [
				                    "elasticmapreduce.amazonaws.com"
				                ]
				            }
				        }
				    ]
				}
				""").build());
		iamClient.attachRolePolicy(AttachRolePolicyRequest.builder()
				.policyArn("arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole").build());

		// This role will be part of the ec2 instance profile, so that nodes in the EMR
		// cluster have needed access.
		iamClient.createRole(CreateRoleRequest.builder().roleName("EMREC2Role").assumeRolePolicyDocument("""
				{
				    "Version": "2012-10-17",
				    "Statement": [
				        {
				            "Effect": "Allow",
				            "Action": [
				                "sts:AssumeRole"
				            ],
				            "Principal": {
				                "Service": [
				                    "ec2.amazonaws.com"
				                ]
				            }
				        }
				    ]
				}
								""").build());
		iamClient.attachRolePolicy(AttachRolePolicyRequest.builder()
				.policyArn("arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role").build());
	}

}
