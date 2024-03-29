import mysql from 'mysql2';

export default class AwsSPConnectDBMySqlUtils {
  mySqlDbConfig;
  marketPlaceIdsMap;
  static initialized = false;

  constructor(dbConfig) {
    this.mySqlDbConfig = dbConfig;
    this.marketPlaceIdsMap = {
      "Amazon.com": "ATVPDKIKX0DER",
      "Amazon.ca": "A2EUQ1WTGCTBG2",
      "Amazon.com.mx": "A1AM78C64UM0Y8",
      "amazon.com": "ATVPDKIKX0DER",
      "amazon.ca": "A2EUQ1WTGCTBG2",
      "amazon.com.mx": "A1AM78C64UM0Y8"
    }
  }

  async init() {
    // the table creation scripts

    // create the ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL table if not exists
    let create_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL_table_sql = "CREATE TABLE IF NOT EXISTS `ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL` (\n" +
      "  `id` int AUTO_INCREMENT NOT NULL,\n" +
      "  `amazon_order_id` varchar(100) NOT NULL,\n" +
      "  `merchant_order_id` varchar(100) DEFAULT NULL,\n" +
      "  `purchase_date` datetime DEFAULT NULL,\n" +
      "  `last_updated_date` datetime DEFAULT NULL,\n" +
      "  `order_status` varchar(45) DEFAULT NULL,\n" +
      "  `fulfillment_channel` varchar(45) DEFAULT NULL,\n" +
      "  `sales_channel` varchar(45) DEFAULT NULL,\n" +
      "  `order_channel` varchar(45) DEFAULT NULL,\n" +
      "  `ship_service_level` varchar(45) DEFAULT NULL,\n" +
      "  `product_name` varchar(100) DEFAULT NULL,\n" +
      "  `sku` varchar(45) DEFAULT NULL,\n" +
      "  `asin` varchar(45) DEFAULT NULL,\n" +
      "  `item_status` varchar(45) DEFAULT NULL,\n" +
      "  `tax_collection_model` varchar(45) DEFAULT NULL,\n" +
      "  `tax_collection_responsible_party` varchar(45) DEFAULT NULL,\n" +
      "  `quantity` varchar(10) DEFAULT NULL,\n" +
      "  `currency` varchar(10) DEFAULT NULL,\n" +
      "  `item_price` varchar(10) DEFAULT NULL,\n" +
      "  `item_tax` varchar(20) DEFAULT NULL,\n" +
      "  `shipping_price` varchar(10) DEFAULT NULL,\n" +
      "  `shipping_tax` varchar(20) DEFAULT NULL,\n" +
      "  `gift_wrap_price` varchar(20) DEFAULT NULL,\n" +
      "  `gift_wrap_tax` varchar(20) DEFAULT NULL,\n" +
      "  `item_promotion_discount` varchar(20) DEFAULT NULL,\n" +
      "  `ship_promotion_discount` varchar(20) DEFAULT NULL,\n" +
      "  `ship_city` varchar(45) DEFAULT NULL,\n" +
      "  `ship_state` varchar(45) DEFAULT NULL,\n" +
      "  `ship_postal_code` varchar(45) DEFAULT NULL,\n" +
      "  `ship_country` varchar(10) DEFAULT NULL,\n" +
      "  `promotion_ids` varchar(45) DEFAULT NULL,\n" +
      "  `is_business_order` varchar(45) DEFAULT NULL,\n" +
      "  `purchase_order_number` varchar(45) DEFAULT NULL,\n" +
      "  `price_designation` varchar(45) DEFAULT NULL,\n" +
      "  `buyer_company_name` varchar(45) DEFAULT NULL,\n" +
      "  `buyer_tax_registration_country` varchar(45) DEFAULT NULL,\n" +
      "  `buyer_tax_registration_type` varchar(45) DEFAULT NULL,\n" +
      "  `is_sold_by_ab` varchar(45) DEFAULT NULL,\n" +
      "  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n" +
      "  `market_place_id` varchar(100) NOT NULL,\n" +
      "  PRIMARY KEY (`id`)\n" +
      ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"

    await this.#execSql(create_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL_table_sql, []);

    let create_GET_MERCHANT_LISTINGS_ALL_DATA_table_sql = "CREATE TABLE IF NOT EXISTS\n" +
      "  `GET_MERCHANT_LISTINGS_ALL_DATA` (\n" +
      "    `item_name` text,\n" +
      "    `item_description` text,\n" +
      "    `listing_id` varchar(50) NOT NULL,\n" +
      "    `seller_sku` varchar(50) DEFAULT NULL,\n" +
      "    `price` varchar(50) DEFAULT NULL,\n" +
      "    `quantity` varchar(50) DEFAULT NULL,\n" +
      "    `open_date` varchar(50) DEFAULT NULL,\n" +
      "    `image_url` varchar(50) DEFAULT NULL,\n" +
      "    `item_is_marketplace` varchar(50) DEFAULT NULL,\n" +
      "    `product_id_type` varchar(50) DEFAULT NULL,\n" +
      "    `zshop_shipping_fee` varchar(50) DEFAULT NULL,\n" +
      "    `item_note` varchar(50) DEFAULT NULL,\n" +
      "    `item_condition` varchar(50) DEFAULT NULL,\n" +
      "    `zshop_category1` varchar(50) DEFAULT NULL,\n" +
      "    `zshop_browse_path` varchar(50) DEFAULT NULL,\n" +
      "    `zshop_storefront_feature` varchar(50) DEFAULT NULL,\n" +
      "    `asin1` varchar(50) DEFAULT NULL,\n" +
      "    `asin2` varchar(50) DEFAULT NULL,\n" +
      "    `asin3` varchar(50) DEFAULT NULL,\n" +
      "    `will_ship_internationally` varchar(50) DEFAULT NULL,\n" +
      "    `expedited_shipping` varchar(50) DEFAULT NULL,\n" +
      "    `zshop_boldface` varchar(50) DEFAULT NULL,\n" +
      "    `product_id` varchar(50) DEFAULT NULL,\n" +
      "    `bid_for_featured_placement` varchar(50) DEFAULT NULL,\n" +
      "    `add_delete` varchar(50) DEFAULT NULL,\n" +
      "    `pending_quantity` varchar(50) DEFAULT NULL,\n" +
      "    `fulfillment_channel` varchar(50) DEFAULT NULL,\n" +
      "    `merchant_shipping_group` varchar(50) DEFAULT NULL,\n" +
      "    `status` varchar(50) DEFAULT NULL,\n" +
      "    `minimum_order_quantity` varchar(50) DEFAULT NULL,\n" +
      "    `sell_remainder` varchar(50) DEFAULT NULL,\n" +
      "    `market_place_id` varchar(100) DEFAULT NULL,\n" +
      "    PRIMARY KEY (`listing_id`)\n" +
      "  ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci";
    await this.#execSql(create_GET_MERCHANT_LISTINGS_ALL_DATA_table_sql, []);

    let create_GET_V2_SETTLEMENT_REPORT_DATA_table_sql = "CREATE TABLE IF NOT EXISTS\n" +
      "  `GET_V2_SETTLEMENT_REPORT_DATA` (\n" +
      "    `settlement_id` varchar(50) NOT NULL,\n" +
      "    `local_index` int NOT NULL,\n" +
      "    `settlement_start_date` date DEFAULT NULL,\n" +
      "    `settlement_end_date` date DEFAULT NULL,\n" +
      "    `deposit_date` date DEFAULT NULL,\n" +
      "    `total_amount` varchar(50) DEFAULT NULL,\n" +
      "    `currency` varchar(50) DEFAULT NULL,\n" +
      "    `transaction_type` varchar(50) DEFAULT NULL,\n" +
      "    `order_id` varchar(50) NOT NULL,\n" +
      "    `merchant_order_id` varchar(50) DEFAULT NULL,\n" +
      "    `adjustment_id` varchar(50) DEFAULT NULL,\n" +
      "    `shipment_id` varchar(50) DEFAULT NULL,\n" +
      "    `marketplace_name` varchar(50) DEFAULT NULL,\n" +
      "    `shipment_fee_type` varchar(50) DEFAULT NULL,\n" +
      "    `shipment_fee_amount` varchar(50) DEFAULT NULL,\n" +
      "    `order_fee_type` varchar(50) DEFAULT NULL,\n" +
      "    `order_fee_amount` varchar(50) DEFAULT NULL,\n" +
      "    `fulfillment_id` varchar(50) DEFAULT NULL,\n" +
      "    `posted_date` date DEFAULT NULL,\n" +
      "    `order_item_code` varchar(50) NOT NULL,\n" +
      "    `merchant_order_item_id` varchar(50) DEFAULT NULL,\n" +
      "    `merchant_adjustment_item_id` varchar(50) DEFAULT NULL,\n" +
      "    `sku` varchar(50) DEFAULT NULL,\n" +
      "    `quantity_purchased` varchar(50) DEFAULT NULL,\n" +
      "    `price_type` varchar(50) DEFAULT NULL,\n" +
      "    `price_amount` varchar(50) DEFAULT NULL,\n" +
      "    `item_related_fee_type` varchar(50) DEFAULT NULL,\n" +
      "    `item_related_fee_amount` varchar(50) DEFAULT NULL,\n" +
      "    `misc_fee_amount` varchar(50) DEFAULT NULL,\n" +
      "    `other_fee_amount` varchar(50) DEFAULT NULL,\n" +
      "    `other_fee_reason_description` varchar(50) DEFAULT NULL,\n" +
      "    `promotion_id` varchar(50) DEFAULT NULL,\n" +
      "    `promotion_type` varchar(50) DEFAULT NULL,\n" +
      "    `promotion_amount` varchar(50) DEFAULT NULL,\n" +
      "    `direct_payment_type` varchar(50) DEFAULT NULL,\n" +
      "    `direct_payment_amount` varchar(50) DEFAULT NULL,\n" +
      "    `other_amount` varchar(50) DEFAULT NULL,\n" +
      "    `market_place_id` varchar(100) DEFAULT NULL,\n" +
      "    PRIMARY KEY (\n" +
      "      `settlement_id`,\n" +
      "      `order_id`,\n" +
      "      `order_item_code`,\n" +
      "      `local_index`\n" +
      "    )\n" +
      "  ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci";
    await this.#execSql(create_GET_V2_SETTLEMENT_REPORT_DATA_table_sql, []);
    AwsSPConnectDBMySqlUtils.initialized = true;
  }

  async execSqlWithInit(sql, values) {
    if (!AwsSPConnectDBMySqlUtils.initialized) {
      await this.init();
    }
    return this.#execSql(sql, values);
  }

  async #execSql(sql, values) {
    if (!this.mySqlDbConfig) {
      throw new Error('Initialize the class with proper mySqlDbConfig first');
    }
    const myConfig = this.mySqlDbConfig;
    const connection_params = Object.assign(
      myConfig,
      {
        connectTimeout: 12000,      // in Milli seconds
        connectionLimit: 2,
        dateStrings: true
      }
    );

    const pool = mysql.createPool(connection_params);

    const poolclose = async () => {
      // You must wait till the pool is closed to avoid intermittent errors
      var p = new Promise(function (resolve, reject) {
        try {
          pool.end(err => {
            if (err)
              console.log('execSql.poolclose.error.warning',
                {sql, error: err});
            resolve(true);
          });
        } catch (err) {
          console.log('execSql.poolclose.error.warning',
            {sql, error: err});
          resolve(true);
        }
      });
      return await p;
    };

    var promise = new Promise(function (resolve, reject) {

      var query = {sql: sql};
      if (values !== undefined) query.values = values;

      pool.query(query,
        async function (error, results, fields) {
          if (error) {
            pool.query(query,
              async function (err, res, flds) {
                if (err) {
                  console.log('execSql.fail', {sql, error: err});
                  await poolclose();
                  reject(err);
                } else {
                  console.log('execSql.retry.OK.info', {});
                  await poolclose();
                  // printResult = printResult && res && (res.length < 1000);
                  // if (printResult)
                  //   console.log('execSQL.retry.result.info', { result: res });
                  resolve(res);
                }
              }
            );
          } else {
            await poolclose();
            resolve(results);
          }
        }
      );
    });

    return await promise;
  }

  async insertReportStatus(args) {
    try {
      let sql = `INSERT INTO reports_status (
            report_type, status, report_id, report_file_id ${args.marketplaceId ? ', market_place_id' : ''}
        ) values ('${args.report_type}','${args.status}','${args.report_id}','${args.report_file_id}' ${args.marketplaceId ? `, '${args.marketplaceId}' ` : ''} )`
      console.log('insertReportStatus.sql', sql)
      let response = await this.execSqlWithInit(sql, [])
      return response;
      console.log('response', response)
    } catch (error) {
      console.error('insertOrdersRecordsInBulk.error', error)
      throw error;
    }
  }

  async updateReportStatus(args) {
    try {
      let sql = `UPDATE reports_status SET status = '${args.status}' where report_id = '${args.report_id}' `;
      console.log('updateReportStatus.sql', sql)
      let response = await this.execSqlWithInit(sql, [])
      console.log('response', response)
    } catch (error) {
      console.error('insertOrdersRecordsInBulk.error', error)
      throw error;
    }
  }

  async fetchPendingReports(args) {
    try {
      let sql = `SELECT * FROM reports_status where status = '${args.status}' or status = 'IN_PROGRESS' or status = 'IN_QUEUE' ORDER BY modified_date desc`;
      console.log('fetchPendingReports.sql', sql)
      let response = await this.execSqlWithInit(sql, [])
      console.log('response.length', response.length)
      return response
    } catch (error) {
      console.error('insertOrdersRecordsInBulk.error', error)
      throw error;
    }
  }

  async insertOrdersRecordsInBulk(orders, market_place_id) {
    console.log('insertOrdersRecordsInBulk.orders', orders[0])
    // remove the data from first and last dates to compensate for timezone differences from UTC
    // first sort the orders array
    orders = orders.sort((a, b) => new Date(a['purchase-date']).getTime() - new Date(b['purchase-date']).getTime())
    // get the first date in YYYY-MM-DD format
    let firstDateStr = new Date(orders[0]['purchase-date']).toISOString().split('T')[0];
    let lastDateStr = new Date(orders[orders.length - 1]['purchase-date']).toISOString().split('T')[0];

    // remove all entries with purchase_date in firstDateStr or lastDateStr
    orders = orders.filter((o) => {
      let YYYYMMDD = new Date(o['purchase-date']).toISOString().split('T')[0];
      if (YYYYMMDD === firstDateStr || YYYYMMDD === lastDateStr) {
        return false;
      }
      return true;
    });

    try {
      // delete all entries from db in date range available in orders
      firstDateStr = new Date(orders[0]['purchase-date']).toISOString().split('T')[0];
      // add one day to it to ensure that hours>0 for the given date also are deleted
      lastDateStr = new Date(orders[orders.length-1]['purchase-date']);
      lastDateStr.setDate(lastDateStr.getDate()+1);
      lastDateStr = lastDateStr.toISOString().split('T')[0];
      let sql = `DELETE from ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL where purchase_date>='${firstDateStr}' and purchase_date<'${lastDateStr}'`;
      await this.execSqlWithInit(sql);
      sql = `REPLACE INTO ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL (
            amazon_order_id,
            merchant_order_id,
            purchase_date,
            last_updated_date,
            order_status,
            fulfillment_channel,
            sales_channel,
            order_channel,
            ship_service_level,
            product_name,
            sku,
            asin,
            item_status,
            tax_collection_model,
            tax_collection_responsible_party,
            quantity,
            currency,
            item_price,
            item_tax,
            shipping_price,
            shipping_tax,
            gift_wrap_price,
            gift_wrap_tax,
            item_promotion_discount,
            ship_promotion_discount,
            ship_city,
            ship_state,
            ship_postal_code,
            ship_country,
            promotion_ids,
            is_business_order,
            purchase_order_number,
            price_designation,
            buyer_company_name,
            buyer_tax_registration_country,
            buyer_tax_registration_type,
            is_sold_by_ab,
            market_place_id
        ) values ?`;
      let values = [];
      orders.map((order) => {
        values.push([`${order['amazon-order-id']}`, `${order['merchant-order-id']}`, order['purchase-date'], order['last-updated-date'], order['order-status'], order['fulfillment-channel'], order['sales-channel'], order['order-channel'], order['ship-service-level'], order['product-name'], order['sku'], order['asin'], order['item-status'], order['tax-collection-model'], order['tax-collection-responsible-party'], order['quantity'], order['currency'], order['item-price'], order['item-tax'], order['shipping-price'], order['shipping-tax'], order['gift-wrap-price'], order['gift-wrap-tax'], order['item-promotion-discount'], order['ship-promotion-discount'], order['ship-city'], order['ship-state'], order['ship-postal-code'], order['ship-country'], order['promotion-ids'], order['is-business-order'], order['purchase-order-number'], order['price-designation'], order['buyer-company-name'], order['buyer-tax-registration-country'], order['buyer-tax-registration-type'], order['is-sold-by-ab'], this.marketPlaceIdsMap[order['sales_channel']]])
      })
      let response = await this.execSqlWithInit(sql, [values])
      console.log('response', response)
    } catch (error) {
      console.error('insertOrdersRecordsInBulk.error', error)
      throw error;
    }
  }

  async insertMerchantListing(dataArr, market_place_id) {
    console.log('=========dataArr', dataArr[0])
    try {
      let sql = 'REPLACE INTO GET_MERCHANT_LISTINGS_ALL_DATA ( item_name, item_description, listing_id, seller_sku, price, quantity, open_date, image_url, item_is_marketplace, product_id_type, zshop_shipping_fee, item_note, item_condition, zshop_category1, zshop_browse_path, zshop_storefront_feature, asin1, asin2, asin3, will_ship_internationally, expedited_shipping, zshop_boldface, product_id, bid_for_featured_placement, add_delete, pending_quantity, fulfillment_channel, merchant_shipping_group, status, minimum_order_quantity, sell_remainder, market_place_id ) values ? ';
      let values = [];
      dataArr.map((data) => {
        values.push([
          data['item-name'],
          data['item-description'],
          data['listing-id'],
          data['seller-sku'],
          data['price'],
          data['quantity'],
          data['open-date'],
          data['image-url'],
          data['item-is-marketplace'],
          data['product-id-type'],
          data['zshop-shipping-fee'],
          data['item-note'],
          data['item-condition'],
          data['zshop-category1'],
          data['zshop-browse-path'],
          data['zshop-storefront-feature'],
          data['asin1'],
          data['asin2'],
          data['asin3'],
          data['will-ship-internationally'],
          data['expedited-shipping'],
          data['zshop-boldface'],
          data['product-id'],
          data['bid-for-featured-placement'],
          data['add-delete'],
          data['pending-quantity'],
          data['fulfillment-channel'],
          data['merchant-shipping-group'],
          data['status'],
          data['Minimum order quantity'],
          data['Sell remainder'],
          `${market_place_id}`
        ])
      })
      console.log('=======values', values[0])
      let response = await this.execSqlWithInit(sql, [values])
      console.log('response', response)
    } catch (error) {
      console.error('insertInventoryData.error', error)
      throw error;
    }
  }

  async insertV2Settlement(dataArr, market_place_id) {
    console.log('=========dataArr.settlement', dataArr[0])
    let sql = 'replace INTO GET_V2_SETTLEMENT_REPORT_DATA ( local_index, settlement_id, settlement_start_date, settlement_end_date, deposit_date, total_amount, currency, transaction_type, order_id, merchant_order_id, adjustment_id, shipment_id, marketplace_name, shipment_fee_type, shipment_fee_amount, order_fee_type, order_fee_amount, fulfillment_id, posted_date, order_item_code, merchant_order_item_id, merchant_adjustment_item_id, sku, quantity_purchased, price_type, price_amount, item_related_fee_type, item_related_fee_amount, misc_fee_amount, other_fee_amount, other_fee_reason_description, promotion_id, promotion_type, promotion_amount, direct_payment_type, direct_payment_amount, other_amount, market_place_id, past ) values ? ';
    let values = [];
    dataArr.map((data, index) => {
      values.push([
        index,
        data['settlement-id'],
        data['settlement-start-date'],
        data['settlement-end-date'],
        data['deposit-date'],
        data['total-amount'],
        data['currency'],
        data['transaction-type'],
        data['order-id'],
        data['merchant-order-id'],
        data['adjustment-id'],
        data['shipment-id'],
        data['marketplace-name'],
        data['shipment-fee-type'],
        data['shipment-fee-amount'],
        data['order-fee-type'],
        data['order-fee-amount'],
        data['fulfillment-id'],
        data['posted-date'],
        data['order-item-code'],
        data['merchant-order-item-id'],
        data['merchant-adjustment-item-id'],
        data['sku'],
        data['quantity-purchased'],
        data['price-type'],
        data['price-amount'],
        data['item-related-fee-type'],
        data['item-related-fee-amount'],
        data['misc-fee-amount'],
        data['other-fee-amount'],
        data['other-fee-reason-description'],
        data['promotion-id'],
        data['promotion-type'],
        data['promotion-amount'],
        data['direct-payment-type'],
        data['direct-payment-amount'],
        data['other-amount'],
        `${market_place_id}`,
        data['past'] || false
      ])
    })
    console.log('=======values', values[0])
    let response = await this.execSqlWithInit(sql, [values])
    console.log('response', response)
  }


  async saveFileData(args) {
    try {
      if (args.report_type == 'GET_FBA_REIMBURSEMENTS_DATA') {
        // let insertFBAReimursmentDataResponse = await insertFBAReimursmentData(args.fileData, args.market_place_id)
      } else if (args.report_type == 'GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA') {
        // let insertFBAFulfillmentDataResponse = await insertFBAFulfillmentData(args.fileData, args.market_place_id)
      } else if (args.report_type == 'GET_FBA_ESTIMATED_FBA_FEES_TXT_DATA') {
        // let insertFBAFeesDataResponse = await insertFBAFeesData(args.fileData, args.market_place_id)
      } else if (args.report_type == 'GET_FBA_FULFILLMENT_CURRENT_INVENTORY_DATA') {
        // let insertInventoryDataResponse = await insertInventoryData(args.fileData, args.market_place_id)
      } else if (args.report_type == 'GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL') {
        let insertOrdersDataResponse = await this.insertOrdersRecordsInBulk(args.fileData)
      } else if (args.report_type == 'GET_MERCHANT_LISTINGS_ALL_DATA') {
        let insertMerchantListingResponse = await this.insertMerchantListing(args.fileData, args.market_place_id)
      } else if (args.report_type == 'GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE') {
        let insertV2SettlementResponse = await this.insertV2Settlement(args.fileData, args.market_place_id)
      } else if (args.report_type == 'GET_FBA_MYI_ALL_INVENTORY_DATA') {
        // let insertV2SettlementResponse = await insertMyiInventoryData(args.fileData, args.market_place_id)
      }

    } catch (error) {
      console.log('saveFileData.error', error)
      throw error;
    }
  }
}


