#!/usr/bin/env node
import "source-map-support/register";
import * as cdk from "aws-cdk-lib";
import { CdkStreamingApplicationStack } from "../lib/cdk-streaming-application-stack";

const app = new cdk.App();
const stack = new CdkStreamingApplicationStack(app, "CdkStreamingApplicationStack", {
  env: { account: process.env.CDK_DEFAULT_ACCOUNT, region: process.env.CDK_DEFAULT_REGION },
});

cdk.Tags.of(stack).add('Project', 'CDK Streaming Application')