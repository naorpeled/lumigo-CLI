const _ = require("lodash");
const { getAWSSDK } = require("../lib/aws");
const { getQueueUrl } = require("../lib/sqs");
const { getTopicArn } = require("../lib/sns");
const { Command, flags } = require("@oclif/command");
const { checkVersion } = require("../lib/version-check");
const { track } = require("../lib/analytics");

class ExportSqsToFile extends Command {
	async run() {
		const { flags } = this.parse(ExportSqsToFile);
		const {
			queueName,
			targetPath,
			region,
			concurrency,
			profile,
			keep,
			httpProxy
		} = flags;

		global.region = region;
		global.profile = profile;
		global.httpProxy = httpProxy;
		global.keep = keep;
		const AWS = getAWSSDK();
		global.SQS = new AWS.SQS();

		checkVersion();

		track("export-sqs-to-file", { region, targetPath, concurrency, keep });

		this.log(`finding the SQS [${queueName}] in [${region}]`);
		const queueUrl = await getQueueUrl(queueName);

		this.log(
			`exporting events content from [${queueUrl}] to [${targetPath}] with ${concurrency} concurrent pollers`
		);
		await this.getMessages(queueUrl, concurrency);

		this.log("all done!");
	}

	async getMessages(dlqQueueUrl, concurrency) {
		const promises = _.range(0, concurrency).map(() =>
			this.runPoller(dlqQueueUrl)
		);
		await Promise.all(promises);
	}

	async runPoller(dlqQueueUrl) {
		let emptyReceives = 0;
		let seenMessageIds = new Set();

		// eslint-disable-next-line no-constant-condition
		while (true) {
			const resp = await global.SQS.receiveMessage({
				QueueUrl: dlqQueueUrl,
				MaxNumberOfMessages: 10,
				MessageAttributeNames: ["All"]
			}).promise();

			if (_.isEmpty(resp.Messages)) {
				emptyReceives += 1;

				// if we don't receive anything 10 times in a row, assume the queue is empty
				if (emptyReceives >= 10) {
					break;
				} else {
					continue;
				}
			}

			emptyReceives = 0;

			if (global.keep) {
				resp.Messages.forEach(msg => seenMessageIds.add(msg.MessageId));
			} else {
				const deleteEntries = resp.Messages.map(msg => ({
					Id: msg.MessageId,
					ReceiptHandle: msg.ReceiptHandle
				}));
				await global.SQS.deleteMessageBatch({
					QueueUrl: dlqQueueUrl,
					Entries: deleteEntries
				}).promise();
			}

			return resp.Messages;
		}
	}
}

ExportSqsToFile.description =
	"Replays the messages in a SQS DLQ back to the main queue";
ExportSqsToFile.flags = {
	dlqQueueName: flags.string({
		char: "d",
		description: "name of the SQS DLQ queue, e.g. task-queue-dlq-dev",
		required: true
	}),
	targetName: flags.string({
		char: "n",
		description: "name of the target SQS queue/SNS topic, e.g. task-queue-dev",
		required: true
	}),
	targetType: flags.string({
		char: "t",
		description: "valid values are SQS [default], SNS, and Kinesis",
		required: false,
		options: ["SQS", "SNS", "Kinesis"],
		default: "SQS"
	}),
	targetProfile: flags.string({
		description: "AWS CLI profile name to use for the target account",
		required: false
	}),
	targetRegion: flags.string({
		description: "AWS region for the target resource, e.g. eu-west-1",
		required: false
	}),
	region: flags.string({
		char: "r",
		description: "AWS region, e.g. us-east-1",
		required: true
	}),
	concurrency: flags.integer({
		char: "c",
		description: "how many concurrent pollers to run",
		required: false,
		default: 10
	}),
	profile: flags.string({
		char: "p",
		description: "AWS CLI profile name",
		required: false
	}),
	keep: flags.boolean({
		char: "k",
		description: "whether to keep the replayed messages in the DLQ",
		required: false,
		default: false
	}),
	httpProxy: flags.string({
		description: "URL of the http/https proxy (when running in a corporate network)",
		required: false
	})
};

module.exports = ExportSqsToFile;
