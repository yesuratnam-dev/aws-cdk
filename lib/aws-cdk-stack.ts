import * as cdk from 'aws-cdk-lib';
import { Runtime } from 'aws-cdk-lib/aws-lambda';
import { Bucket } from 'aws-cdk-lib/aws-s3';
// import { Lambda } from 'aws-cdk-lib/aws-ses-actions';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3n from 'aws-cdk-lib/aws-s3-notifications';
// import * as sqs from 'aws-cdk-lib/aws-sqs';

export class AwsCdkStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const s3Bucket = new Bucket(this, "LambdaTriggerBucket",{
      versioned: true,
      publicReadAccess: false,
      bucketName: "kuyesura-dev-7",
      // removalPolicy: cdk.RemovalPolicy.DESTROY,
      // autoDeleteObjects: true

    });

    const func = new lambda.Function(this, "MyFuncS3Trigger",{
      runtime : lambda.Runtime.NODEJS_18_X,
      functionName: "MyFuncS3EventTrigger",
      code: lambda.Code.fromInline(`
        exports.handler = async (event) => {
          console.log('Event:', JSON.stringify(event, null, 2));
          return {
            statusCode: 200,
            body: 'Hello from Lambda!'
          };
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
  }
}
