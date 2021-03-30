const _ = require("lodash");
const { Command, flags } = require("@oclif/command");
const { checkVersion } = require("../lib/version-check");
const { track } = require("../lib/analytics");
const inquirer = require("inquirer");
const lambda = require("../lib/scanners/lambda");
const sns = require("../lib/scanners/sns");
const sqs = require("../lib/scanners/sqs");
const dynamodb = require("../lib/scanners/dynamodb");
const axios = require("axios");
require("colors");
const { getAWSSDK } = require("../lib/aws");

const AWS = getAWSSDK();
const STS = new AWS.STS();

class ScannerCommand extends Command {
	async run() {
		const { flags } = this.parse(ScannerCommand);
		const { profile, httpProxy } = flags;

		global.profile = profile;
		global.httpProxy = httpProxy;

		checkVersion();

		track("scanner", {});

		this.log(`
Hi, welcome to the Lumigo scanner!
    
This command scans AWS resources in the AWS account you're connected to
and recommends areas that can be improved.

A customized report will be sent to your email.
`);

		this.log("We will send metadata about AWS resources to Lumigo.".yellow);

		const { toProceed } = await inquirer.prompt([
			{
				type: "confirm",
				name: "toProceed",
				message:
					"Do you give Lumigo permission to receive metadata about AWS resources?"
			}
		]);

		if (!toProceed) {
			return;
		}

		const email = await this.signIn().catch(err => {
			throw err;
		});

		this.log(`
Wonderful, you're all set.

We will be scanning your AWS account for relevant resources:
  * DynamoDB tables
  * Lambda functions
  * SNS topics
  * SQS queues
  * etc.
`);

		this.log("Scanning account for relevant resources...\n");
		const resources = await this.scan();

		const resourceCount = _.flatten(Object.values(resources)).length;

		this.log(`
Found ${resourceCount} AWS resources.

Sending metadata to Lumigo for processing...
(look for an email from us in your inbox soon)
`);
		const caller = await STS.getCallerIdentity().promise();
		await axios({
			method: "post",
			url: "https://001sg6kjid.execute-api.us-west-2.amazonaws.com/prod/scan",
			data: { resources, awsAccountId: caller.Account, email }
		});
	}

	async signIn() {
		let email;

		const emailResp = await inquirer.prompt([
			{
				type: "input",
				name: "email",
				message: "Send the report to this email"
			}
		]);

		email = emailResp.email;

		return email;
	}

	async scan() {
		const resources = await Promise.all([
			lambda.getLambdaFunctions(),
			sns.getSnsTopics(),
			sqs.getSqsQueues(),
			dynamodb.getDynamoDBTables()
		]);

		return resources.reduce((acc, next) => {
			return Object.assign(acc, next);
		}, {});
	}
}

ScannerCommand.description = "Use Lumigo Stackoscope to scan your AWS account and suggest improvements";
ScannerCommand.flags = {
	profile: flags.string({
		char: "p",
		description: "AWS CLI profile name",
		required: false
	}),
	httpProxy: flags.string({
		description: "URL of the http/https proxy (when running in a corporate network)",
		required: false
	})
};

module.exports = ScannerCommand;
