import * as path from "path";
import { CfnOutput, Duration, RemovalPolicy, Stack, StackProps } from "aws-cdk-lib";
import { Construct } from "constructs";
import * as kinesis from "aws-cdk-lib/aws-kinesis";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as iam from "aws-cdk-lib/aws-iam";
import * as s3 from "aws-cdk-lib/aws-s3";
import { KinesisEventSource } from "aws-cdk-lib/aws-lambda-event-sources";
import * as kinesisanalytics from "aws-cdk-lib/aws-kinesisanalytics";
import * as kinesisfirehose from 'aws-cdk-lib/aws-kinesisfirehose';

export class CdkStreamingApplicationStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    var groceryOrderStream = new kinesis.Stream(this, "GroceryOrderStream", {
      streamName: "GroceryOrderStream",
      shardCount: 1,
    });

    var enrichedOrderStream = new kinesis.Stream(this, "EnrichedOrderStream", {
      streamName: "EnrichedOrderStream",
      shardCount: 1,
    });

    var userInfoTable = new dynamodb.Table(this, "UserInfoTable", {
      partitionKey: { name: "user_id", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: RemovalPolicy.DESTROY, // For development purpose only
    });

    var enrichOrderProcessor = new lambda.Function(this, "EnrichOrderProcessor", {
      functionName: "EnrichOrderProcessor",
      code: lambda.Code.fromAsset(path.join(__dirname, "..", "processors")),
      handler: "enricher.lambda_handler",
      runtime: lambda.Runtime.PYTHON_3_9,
      timeout: Duration.seconds(10),
      environment: {
        USER_INFO_TABLE: userInfoTable.tableName,
        DESTINATION_STREAM: enrichedOrderStream.streamName,
      },
    });

    enrichOrderProcessor.addEventSource(
      new KinesisEventSource(groceryOrderStream, {
        batchSize: 100, // default
        startingPosition: lambda.StartingPosition.TRIM_HORIZON,
        parallelizationFactor: 10,
        bisectBatchOnError: true,
        retryAttempts: 3
      })
    );

    userInfoTable.grantReadData(enrichOrderProcessor);
    enrichedOrderStream.grantWrite(enrichOrderProcessor);


    var newLineAppender = new lambda.Function(this, "NewLineAppender", {
      functionName: "NewLineAppender",
      code: lambda.Code.fromAsset(path.join(__dirname, "..", "processors")),
      handler: "newline-appender.lambda_handler",
      runtime: lambda.Runtime.PYTHON_3_9,
    });


    const enrichedOrdersBucket = new s3.Bucket(this, 'EnrichedOrdersBucket', {
      removalPolicy: RemovalPolicy.DESTROY,
      bucketName: 'enriched-orders-bucket-lluim',
    });

    const firehoseRole = new iam.Role(this, 'FirehoseRole', {
        assumedBy: new iam.ServicePrincipal('firehose.amazonaws.com')
    });

    enrichedOrderStream.grantRead(firehoseRole);
    enrichedOrderStream.grant(firehoseRole, 'kinesis:DescribeStream');
    enrichedOrdersBucket.grantReadWrite(firehoseRole);
    newLineAppender.grantInvoke(firehoseRole);

    const cfnEnrichedOrdersPersistFirehose = new kinesisfirehose.CfnDeliveryStream(this, 'EnrichedOrdersPersistFirehose', {
      deliveryStreamType: 'KinesisStreamAsSource',
      deliveryStreamName: 'EnrichedOrdersPersistFirehose',

      kinesisStreamSourceConfiguration: {
        kinesisStreamArn: enrichedOrderStream.streamArn,
        roleArn: firehoseRole.roleArn,
      },

      extendedS3DestinationConfiguration: {
        bucketArn: enrichedOrdersBucket.bucketArn,
        roleArn: firehoseRole.roleArn,
    
        bufferingHints: {
          intervalInSeconds: 300,
          sizeInMBs: 100,
        },
        errorOutputPrefix: 'error/',
        prefix: 'data/',
        processingConfiguration: {
          enabled: true,
          processors: [{
            type: 'Lambda',
            parameters: [{
              parameterName: 'LambdaArn',
              parameterValue: newLineAppender.functionArn,
            }],
          }],
        },
      },

      tags: [{
        key: 'Project',
        value: 'CDK Streaming Application',
      }],
    });

    cfnEnrichedOrdersPersistFirehose.node.addDependency(enrichedOrdersBucket);
    cfnEnrichedOrdersPersistFirehose.node.addDependency(firehoseRole);
    cfnEnrichedOrdersPersistFirehose.node.addDependency(enrichedOrderStream);

    const groceryOrderStreamPushUser = new iam.User(this, "GroceryOrderStreamPushUser", {
      userName: "GroceryOrderStreamPushUser",
    });
    groceryOrderStream.grantWrite(groceryOrderStreamPushUser);

    var groceryOrderStreamPushUserAccessKey = new iam.CfnAccessKey(this, "DeveloperS3AccessKey", {
      userName: groceryOrderStreamPushUser.userName,
    });

    new CfnOutput(this, "groceryOrderStreamPushUserAccessKey", {
      value: groceryOrderStreamPushUserAccessKey.ref,
    });
    new CfnOutput(this, "groceryOrderStreamPushUserSecretAccessKey", {
      value: groceryOrderStreamPushUserAccessKey.getAtt("SecretAccessKey").toString(),
    });
  }
}
