import AwsSPConnectDBMySqlUtils from "./lib/db/AwsSPConnectDBMySqlUtils.js";
import AwsSpApiReportsUtils from "./lib/sp-api/AwsSpApiReportsUtils.js";

export default class EComAwsSpApiUtil {
  constructor(configOptions, mySqlDbConfig) {
    this.awsSPConnectDBMySqlUtils = new AwsSPConnectDBMySqlUtils(mySqlDbConfig);
    this.awsSpApiReportsUtils = new AwsSpApiReportsUtils(configOptions);
  }

  static async getInstance(configOptions, mySqlDbConfig) {
    if (!this.instance) {
      this.instance = new EComAwsSpApiUtil(configOptions, mySqlDbConfig);
      await this.instance.awsSPConnectDBMySqlUtils.init();
    }

    return this.instance;
  }
}
