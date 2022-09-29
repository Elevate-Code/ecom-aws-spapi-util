"use strict";
import SellingPartnerAPI from 'amazon-sp-api';

export default class  AwsSpApiReportsUtils {
  configOptions;

  constructor(configOptions) {
    this.configOptions = configOptions;
  }

  async getAccessToken() {
    let sellingPartner = new SellingPartnerAPI({
      region: 'na',
      refresh_token: this.configOptions.refresh_token,
      credentials: {
        SELLING_PARTNER_APP_CLIENT_ID: this.configOptions.credentials.SELLING_PARTNER_APP_CLIENT_ID,
        SELLING_PARTNER_APP_CLIENT_SECRET: this.configOptions.credentials.SELLING_PARTNER_APP_CLIENT_SECRET,
        AWS_ACCESS_KEY_ID: this.configOptions.credentials.AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY: this.configOptions.credentials.AWS_SECRET_ACCESS_KEY,
        AWS_SELLING_PARTNER_ROLE: this.configOptions.credentials.AWS_SELLING_PARTNER_ROLE,
      },
      options: {
        auto_request_tokens: true
      }
    });
    await sellingPartner.refreshAccessToken();
    await sellingPartner.refreshRoleCredentials();
    let access_token = sellingPartner.access_token;
    let role_credentials = sellingPartner.role_credentials;
    console.log('============')
    console.log('access_token', access_token)
    console.log('============')
    console.log('role_credentials', role_credentials)
    console.log('============')
    return sellingPartner
  }

  async createGetMerchantListingsAllDataRequest() {
    try {
      let date = new Date();
      date.setDate(date.getDate() - 5);
      let createRequestResponse = await this.createReportRequest({
        reportType: 'GET_MERCHANT_LISTINGS_ALL_DATA',
        dataStartTime: date.toISOString(),
        marketplaceIdsList: this.configOptions.marketplaceIdsList
      })
      console.log('createRequestResponse', createRequestResponse)
      if (createRequestResponse && createRequestResponse.reportId) {
        return this.createApiResponse(createRequestResponse);
      } else {
        console.log('getMerchantListingsAllData.error', 'Report id not found')
      }
    } catch (error) {
      console.log('getMerchantListingsAllData.error', error)
      return this.createApiResponse(null, error);
    }
    return this.createApiResponse(null, new Error('Report id not found'));
  }

  async getV2SettlementReportDataFlatFile(event) {
    console.log('getV2SettlementReportDataFlatFile.event', event)
    try {
      let scheduledReportsList = this.getReports({ report_type: 'GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE', marketplaceIdsList: this.configOptions.marketplaceIdsList })
      console.log('scheduledReportsList', scheduledReportsList.length)
      if (scheduledReportsList && Array.isArray(scheduledReportsList) && scheduledReportsList.length > 0) {
        console.log('===scheduledReportsList', scheduledReportsList)
        return this.createApiResponse(scheduledReportsList);
      } else {
        console.log("scheduledReportsList is empty", scheduledReportsList);
      }

    } catch (error) {
      console.log('getV2SettlementReportDataFlatFile.error', error)
    }
  }


  async createAllOrderDatabyOrderDateGeneralRequest() {
    try {
      let date = new Date();
      date.setDate(date.getDate() - 5);
      let createRequestResponse = await this.createReportRequest({
        reportType: 'GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL',
        dataStartTime: date.toISOString(),
        marketplaceIdsList: this.configOptions.marketplaceIdsList
      })
      console.log('createRequestResponse', createRequestResponse)
      if (createRequestResponse && createRequestResponse.reportId) {
        return this.createApiResponse(createRequestResponse);
      } else {
        console.log('allOrderDatabyOrderDateGeneral.error', 'Report id not found')
      }
    } catch (error) {
      console.log('allOrderDatabyOrderDateGeneral.error', error)
      return this.createApiResponse(null, error);
    }
    return this.createApiResponse(null, new Error('Report id not found'));
  }

  async downloadFile(args) {
    try {
      let sellingPartner = new SellingPartnerAPI(this.configOptions);
      let res = await sellingPartner.callAPI({
        api_path: `/reports/2020-09-04/documents/${args.documentId}`,
        method: 'GET'
      });
      let report = await sellingPartner.download(res, {
        json: true
      });
      return this.createApiResponse(report);
    } catch (error) {
      console.log('downloadFile.error', error)
      return this.createApiResponse(null, error);
    }
  }

  async checkDocumentStatus(args) {
    let response = {}
    try {
      let sellingPartner = new SellingPartnerAPI(this.configOptions);
      let res = await sellingPartner.callAPI({
        operation: 'getReport',
        endpoint: 'reports',
        method: 'GET',
        path: {reportId: `${args.reportId}`}
      });
      console.log(res);
      return this.createApiResponse(res)
    } catch (error) {
      console.log('checkDocumentStatus.error', error)
      return this.createApiResponse(null, error);
    }
  }

  async createReportRequest(args) {
    console.log("createReportRequest called with ", args);
    let response = {}
    try {
      let sellingPartner = new SellingPartnerAPI(this.configOptions);
      let res = await sellingPartner.callAPI({
        operation: 'createReport',
        endpoint: 'reports',
        body: {
          reportType: args.reportType,
          dataStartTime: args.dataStartTime,
          ...(args.marketplaceIdsList && {marketplaceIds: args.marketplaceIdsList})
        },
      });
      console.log('body', {
        reportType: args.reportType,
        dataStartTime: args.dataStartTime,
        ...(args.marketplaceIdsList && {marketplaceIds: args.marketplaceIdsList})
      })
      console.log('res', res)
      return res
    } catch (error) {
      console.log('createReportRequest.error', error)
      response.error = error.message
    }
    return response;
  }

  async createApiResponse(data, error = null) {
    let obj = {
      success: false
    };
    if (data) {
      obj.success = true
    } else {
      obj.success = false
    }
    return {
      data, ...obj, error
    }
  }

  async getReports(args) {
    try {
      let sellingPartner = new SellingPartnerAPI(this.configOptions);
      let res = await sellingPartner.callAPI({
        operation: 'getReports',
        endpoint: 'reports',
        query: {
          reportTypes: [args.report_type],
          marketplaceIds: args.marketplaceIdsList
        }
      });
      console.log('getReports.res', res);
      return res
    } catch (e) {
      console.log(e);
    }
  }
}


