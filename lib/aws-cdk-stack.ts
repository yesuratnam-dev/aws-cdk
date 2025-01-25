import * as cdk from 'aws-cdk-lib';
import { Runtime } from 'aws-cdk-lib/aws-lambda';
import { Bucket } from 'aws-cdk-lib/aws-s3';
// import { Lambda } from 'aws-cdk-lib/aws-ses-actions';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3n from 'aws-cdk-lib/aws-s3-notifications';
// import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as glue from 'aws-cdk-lib/aws-glue'
import * as s3deploy from 'aws-cdk-lib/aws-s3-deployment';
import * as s3assets from 'aws-cdk-lib/aws-s3-assets';
import * as logs from 'aws-cdk-lib/aws-logs';

export class AwsCdkStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const s3Bucket = new Bucket(this, "LambdaTriggerBucket",{
      versioned: true,
      publicReadAccess: false,
      bucketName: "kuyesura-dev-9"
      // removalPolicy: cdk.RemovalPolicy.DESTROY,
      // autoDeleteObjects: true

    });

    const sourceBucket = new Bucket(this, "GlueSourceBucket",{
      versioned: true,
      publicReadAccess: false,
      bucketName: "kuyesura-dev-9-source-bucket"
      // removalPolicy: cdk.RemovalPolicy.DESTROY,
      // autoDeleteObjects: true

    });

    const destinationBucket = new Bucket(this, "GlueDestinationBucket",{
      versioned: true,
      publicReadAccess: false,
      bucketName: "kuyesura-dev-9-destination-bucket"
      // removalPolicy: cdk.RemovalPolicy.DESTROY,
      // autoDeleteObjects: true

    });

    const codeBucket = new Bucket(this, "CodeBucket",{
      versioned: true,
      publicReadAccess: false,
      bucketName: "kuyesura-dev-9-code-bucket"
      // removalPolicy: cdk.RemovalPolicy.DESTROY,
      // autoDeleteObjects: true
    });

    new s3deploy.BucketDeployment(this, 'scripts', {
      sources: [
        s3deploy.Source.asset('src/code/'),
        // s3deploy.Source.asset('../path/to/second/file')
      ],
      destinationBucket: codeBucket,
      // destinationKeyPrefix: 'multiple/files/'
    });



    const func = new lambda.Function(this, "MyFuncS3Trigger",{
      runtime : lambda.Runtime.NODEJS_18_X,
      functionName: "MyFuncS3EventTrigger",
      code: lambda.Code.fromInline(`
        const AWS = require('@aws-sdk/client-s3');
        const s3 = new AWS.S3();

        exports.handler = async (event) => {
          console.log("Event:", JSON.stringify(event, null, 2));
          
          // Extract bucket name and object key
          const bucketName = event.Records[0].s3.bucket.name;
          const objectKey = event.Records[0].s3.object.key;

          try {
            // Get the file content from S3
            const response = await s3.getObject({
              Bucket: bucketName,
              Key: objectKey
            }).promise();
            
            const fileContent = response.Body.toString('utf-8');
            console.log("File Content:", fileContent);

            // Process file content (e.g., parse JSON or CSV)
            return {
              statusCode: 200,
              body: \`Processed file \${objectKey} from bucket \${bucketName}\`
            };
          } catch (error) {
            console.error("Error:", error);
            throw error;
          }
        };
      `), // Inline code for demonstration
      handler: 'index.handler',
    });

    // Grant Lambda permissions to read the S3 bucket
    s3Bucket.grantRead(func);

    // Add S3 event notification to trigger Lambda on object creation
    s3Bucket.addEventNotification(
      s3.EventType.OBJECT_CREATED,
      new s3n.LambdaDestination(func)
    );

    const glueRole = new cdk.aws_iam.Role(this, 'GlueRole', {
      assumedBy: new cdk.aws_iam.ServicePrincipal('glue.amazonaws.com'),
    });
    
    // Add AWS managed policies for full access
    glueRole.addManagedPolicy(
      cdk.aws_iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole')
    );
    
    glueRole.addManagedPolicy(
      cdk.aws_iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonS3FullAccess')
    );
    
    glueRole.addManagedPolicy(
      cdk.aws_iam.ManagedPolicy.fromAwsManagedPolicyName('CloudWatchFullAccess')
    );
    
    glueRole.addManagedPolicy(
      cdk.aws_iam.ManagedPolicy.fromAwsManagedPolicyName('AWSLambda_FullAccess')
    );


    const customLogGroup = new logs.LogGroup(this, 'CustomGlueJobLogGroup', {
      logGroupName: 'MyGlueJob_Logs', // Your custom log group name
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY
    });

    const glueJob = new glue.CfnJob(this, 'GlueJob', {
      name: 'MyGlueJob',
      role: glueRole.roleArn,
      command: {
        name: 'glueetl',
        scriptLocation: `s3://${codeBucket.bucketName}/script.py`,
      },
      glueVersion: '5.0',
      workerType: 'G.1X',
      numberOfWorkers: 2,
      executionProperty: {
        maxConcurrentRuns: 1,
      },
      defaultArguments: {
        '--source_bucket': sourceBucket.bucketName,
        '--destination_bucket': destinationBucket.bucketName,
        // Configure custom logging
        '--enable-metrics': 'true',
        '--enable-job-insights': 'true',
    
    // Specify custom log group
        '--continuous-log-logGroup': 'MyGlueJob_Logs',
        '--custom-log-stream-prefix': 'glue-job-',
        '--enable-observability-metrics': 'true',
        '--enable-continuous-cloudwatch-log': 'true'
      },
    });

    const scala_glueJob = new glue.CfnJob(this, 'Scala_GlueJob', {
      name: 'MyScalaGlueJob',
      role: glueRole.roleArn,
      command: {
        name: 'glueetl',
        scriptLocation: `s3://${codeBucket.bucketName}/scalascript.scala`,
        pythonVersion: '3'
      },
      glueVersion: '4.0',
      workerType: 'G.1X',
      numberOfWorkers: 2,
      executionProperty: {
        maxConcurrentRuns: 1,
      },
      defaultArguments: {
        // Configure custom logging
        '--enable-metrics': 'true',
        '--enable-job-insights': 'true',
        '--job-language': 'scala',
        '--enable-observability-metrics': 'true',
        '--enable-continuous-cloudwatch-log': 'true',
    
    // Specify custom log group
        '--continuous-log-logGroup': 'MyGlueJob_Logs',
        '--continuous-log-logStreamPrefix': 'glue-job-',
        '--enable-continuous-logging': 'true',
        '--SOURCE_PATH': `s3://${sourceBucket.bucketName}/`,
        '--DESTINATION_PATH': `s3://${destinationBucket.bucketName}/data/`
      },
    });
  }
}
