from collections.abc import Mapping
import re
from typing import Dict, Any, List, Optional, Tuple
import logging,time
_logger = logging.getLogger(__name__)


class OrderQueryHelper:
    def __init__(self, client, **kwargs):
        self.client = client
        self.use_presentment = kwargs.get('order_visible_currency', False)
        self.settings = kwargs

    @staticmethod
    def rest_order_fields():
        """
        Returns a string of Shopify Admin GraphQL Order fields, formatted with 20 fields per line for readability.
        """
        order_fields = {
            "basic": '''
                        id app { id } clientIp cancelReason cancelledAt closedAt confirmationNumber confirmed email createdAt discountCodes displayFinancialStatus displayFulfillmentStatus edited estimatedTaxes customerJourneySummary { lastVisit { id landingPage landingPageHtml occurredAt referralCode referralInfoHtml referrerUrl source sourceDescription sourceType utmParameters { campaign content medium source term } } } 
                        name note statusPageUrl paymentGatewayNames phone presentmentCurrencyCode processedAt sourceIdentifier sourceName tags taxExempt taxLines { channelLiable rate ratePercentage source title priceSet { presentmentMoney { amount currencyCode } shopMoney { amount currencyCode } } } taxesIncluded test totalCashRoundingAdjustment { paymentSet { presentmentMoney { amount currencyCode } shopMoney { amount currencyCode } } refundSet { presentmentMoney { amount currencyCode } shopMoney { amount currencyCode } } } 
                        totalDiscountsSet { presentmentMoney { amount currencyCode } shopMoney { amount currencyCode } } totalPriceSet { presentmentMoney { amount currencyCode } shopMoney { amount currencyCode } } totalOutstandingSet { presentmentMoney { amount currencyCode } shopMoney { amount currencyCode } } totalShippingPriceSet { presentmentMoney { amount currencyCode } shopMoney { amount currencyCode } } totalTaxSet { presentmentMoney { amount currencyCode } shopMoney { amount currencyCode } } totalTipReceivedSet { shopMoney { currencyCode amount } presentmentMoney { amount currencyCode } } totalWeight updatedAt unpaid 
                        shippingLines(first: 10) { nodes { id carrierIdentifier code custom source discountedPriceSet { presentmentMoney { amount currencyCode } shopMoney { amount currencyCode } } shippingRateHandle title taxLines { title source ratePercentage rate priceSet { presentmentMoney { amount currencyCode } shopMoney { amount currencyCode } } } originalPriceSet { presentmentMoney { amount currencyCode } shopMoney { amount currencyCode } } isRemoved discountAllocations { allocatedAmountSet { presentmentMoney { amount currencyCode } shopMoney { amount currencyCode } } } } }
                    '''
            ,
            "customer_data": '''
                                id billingAddress { address1 address2 city company coordinatesValidated country countryCodeV2 firstName formatted(withCompany: false, withName: false) formattedArea id lastName latitude longitude name phone province provinceCode timeZone zip validationResultSummary } currencyCode 
                                customer { id createdAt updatedAt state lastName firstName note verifiedEmail multipassIdentifier email taxExempt tags defaultAddress { address1 address2 city company country countryCodeV2 firstName id lastName latitude longitude name province provinceCode timeZone validationResultSummary zip } }
                                shippingAddress { address1 address2 city company coordinatesValidated countryCodeV2 firstName formattedArea id lastName latitude longitude name phone province provinceCode timeZone validationResultSummary zip country }
                            '''
            ,
            "line_items": '''   id lineItems(first: 25) { nodes { id currentQuantity fulfillmentStatus fulfillableQuantity fulfillmentService { serviceName } isGiftCard name originalTotalSet { presentmentMoney { amount currencyCode } shopMoney { amount currencyCode } } originalUnitPriceSet { presentmentMoney { amount currencyCode } shopMoney { amount currencyCode } } requiresShipping product { id } quantity variant { id sku taxable title } 
                                taxLines(first: 25) { channelLiable price priceSet { presentmentMoney { amount currencyCode } shopMoney { amount currencyCode } } rate ratePercentage source title } taxable title discountAllocations { allocatedAmountSet { presentmentMoney { amount currencyCode } shopMoney { amount currencyCode } } } 
                                duties { countryCodeOfOrigin harmonizedSystemCode id price { presentmentMoney { amount currencyCode } shopMoney { amount currencyCode } } taxLines { priceSet { shopMoney { amount currencyCode } presentmentMoney { amount currencyCode } } rate ratePercentage source title } } } }
                          '''
            ,
            "refunds": '''
                              id refunds(first: 25) { createdAt id note legacyResourceId orderAdjustments(first: 50) { nodes { id reason taxAmountSet { presentmentMoney { amount currencyCode } shopMoney { amount currencyCode } } amountSet { presentmentMoney { amount currencyCode } shopMoney { amount currencyCode } } } } duties { amountSet { presentmentMoney { amount currencyCode } shopMoney { amount currencyCode } } originalDuty { countryCodeOfOrigin harmonizedSystemCode id price { presentmentMoney { amount currencyCode } shopMoney { amount currencyCode } } taxLines { channelLiable rate ratePercentage source title priceSet { presentmentMoney { amount currencyCode } shopMoney { amount currencyCode } } } } } 
                              refundLineItems(first: 25) { nodes { id restocked restockType quantity location { id } subtotalSet { presentmentMoney { amount currencyCode } shopMoney { amount currencyCode } } totalTaxSet { presentmentMoney { amount currencyCode } shopMoney { amount currencyCode } } lineItem { id name quantity sku title currentQuantity requiresShipping } } } refundShippingLines(first: 50) { nodes { id shippingLine { id code carrierIdentifier title taxLines { channelLiable rate ratePercentage source title priceSet { presentmentMoney { amount currencyCode } shopMoney { amount currencyCode } } } source shippingRateHandle custom discountAllocations { allocatedAmountSet { presentmentMoney { amount currencyCode } shopMoney { amount currencyCode } } } } subtotalAmountSet { presentmentMoney { amount currencyCode } shopMoney { amount currencyCode } } taxAmountSet { presentmentMoney { amount currencyCode } shopMoney { amount currencyCode } } } } 
                              transactions(first: 25) { nodes { id gateway kind authorizationCode createdAt accountNumber amountSet { presentmentMoney { amount currencyCode } shopMoney { amount currencyCode } } amountV2 { amount currencyCode } errorCode paymentId paymentMethod processedAt status parentTransaction { id } receiptJson test paymentDetails { ... on CardPaymentDetails { avsResultCode } } } } }
                        '''
            ,
            "transactions": '''
                                id transactions(first: 50) { accountNumber amountV2 { amount currencyCode } amountSet { presentmentMoney { amount currencyCode } shopMoney { amount currencyCode } } authorizationCode authorizationExpiresAt createdAt errorCode fees { id rate rateName taxAmount { amount currencyCode } type amount { amount currencyCode } } 
                                formattedGateway gateway id kind manualPaymentGateway manuallyCapturable maximumRefundable multiCapturable parentTransaction { id gateway kind status } 
                                paymentId processedAt receiptJson settlementCurrency settlementCurrencyRate status test 
                                totalUnsettledSet { presentmentMoney { amount currencyCode } shopMoney { amount currencyCode } } paymentIcon { id src originalSrc } }
                            '''
            ,
            "fulfillment": '''
                                id fulfillments(first: 20) { createdAt deliveredAt displayStatus id location { id name createdAt updatedAt fulfillmentService { callbackUrl handle id inventoryManagement serviceName trackingSupport type } } originAddress { address1 address2 city countryCode provinceCode zip } requiresShipping updatedAt status totalQuantity estimatedDeliveryAt service { handle serviceName trackingSupport type } trackingInfo { company number url } 
                                fulfillmentLineItems(first: 20) { nodes { id quantity lineItem { currentQuantity fulfillmentService { serviceName id handle type } fulfillmentStatus isGiftCard name product { id } variant { id sku } vendor variantTitle taxLines(first: 50) { channelLiable priceSet { presentmentMoney { amount currencyCode } shopMoney { amount currencyCode } } rate ratePercentage source title } duties { countryCodeOfOrigin harmonizedSystemCode id } discountAllocations { allocatedAmountSet { presentmentMoney { currencyCode amount } shopMoney { amount currencyCode } } } } } } }          
                            '''
            ,
            "returns_and_risks": '''
                                id returns(first: 10) { nodes { id returnLineItems(first: 10) { nodes { quantity refundableQuantity refundedQuantity returnReason returnReasonNote ... on ReturnLineItem { id fulfillmentLineItem { id lineItem { id } } } } } } }
                                risk { assessments { riskLevel facts { description sentiment } provider { id title webhookApiVersion } } recommendation }
                                
                                '''
            ,
            "fulfillment_orders": '''
                                id fulfillmentOrders(first: 20) { nodes { orderId orderName status lineItems(first: 25) { nodes { id lineItem { id } }} assignedLocation { location { id }}      }}
                                '''
        }
        return order_fields

    def get_order_count(self, filters):
        shopify_query = (
            f"fulfillment_status:{filters['fulfillment_status']} "
            f"updated_at:>='{filters['updated_at_min']}' "
            f"updated_at:<='{filters['updated_at_max']}'"
        )
        query = f"""
                {{
                  ordersCount(query: "{shopify_query}") {{
                    count
                  }}
                }}"""
        result = self.client.execute(query)
        if result and 'data' in result:
            return result['data'].get('ordersCount', {}).get('count', 0)
        return 0

    def get_order(self, order_ids: List[str]):
        fields = self.rest_order_fields()
        fields_str = "\n".join(value for value in fields.values())
        response = []
        for order_id in order_ids:
            query = f'''
            {{
              order(id: "gid://shopify/Order/{order_id}") {{
                {fields_str}
              }}
            }}
            '''
            order =  self.client.execute(query)
            raw_graphql_order = order.get('data', {}).get('order', {})
            rest_order_data = self._convert_graphql_to_rest_fields(raw_graphql_order)
            rest_order_data['order_api_name'] = 'fetched_via_graphql'
            if 'transactions' in rest_order_data:
                rest_order_data['transaction'] = rest_order_data.pop('transactions')
            response.append(rest_order_data)
        return response

    def list_orders(self, filters):
        """
        Fetches all orders in pages for each field group, merges data by order ID.
        Returns: {order_id: [order_data_dict, ...]} (list contains dicts from each field group/page)
        """
        order_fields = self.rest_order_fields()
        shopify_query = (
            f"fulfillment_status:{filters['fulfillment_status']} "
            f"updated_at:>='{filters['updated_at_min']}' "
            f"updated_at:<='{filters['updated_at_max']}'"
            # f"status:{filters['status']}"
        )
        merged_orders = {}
        start_time = time.time()
        for group, fields in order_fields.items():
            group_orders = self._fetch_all_orders_for_fields(fields, shopify_query, filters.get("limit", 100))
            for order_id, order_list in group_orders.items():
                if order_id not in merged_orders:
                    merged_orders[order_id] = []
                merged_orders[order_id].extend(order_list)
        response = []
        for order_id, order_list in merged_orders.items():
            rest_order_data = self._convert_graphql_to_rest_fields(self.deep_merge_dicts(order_list))
            rest_order_data['order_api_name'] = 'fetched_via_graphql'
            if 'transactions' in rest_order_data:
                rest_order_data['transaction'] = rest_order_data.pop('transactions')
            response.append(rest_order_data)
        end_time = time.time()
        _logger.info(
            f"Total time taken to fetch and merge orders: {end_time - start_time} seconds for date range {filters['updated_at_min']} to {filters['updated_at_max']}")
        return response

    def _fetch_all_orders_for_fields(self, fields, shopify_query, limit):
        """
        Internal helper to fetch all pages for given fields, returns {order_id: [order_data_dict, ...]}
        """
        orders_by_id = {}
        has_next_page = True
        end_cursor = None
        while has_next_page:
            after = f', after: "{end_cursor}"' if end_cursor else ""
            query = f'''
            {{
                orders(first: {limit}{after}, query: "{shopify_query}" sortKey: UPDATED_AT) {{
                    pageInfo {{ endCursor hasNextPage }}
                    nodes {{
                        {fields}
                    }}
                }}
            }}
            '''
            result = self.client.execute(query)
            if 'errors' in result:
                error_details = result['errors'][0] if result['errors'] else 'Unknown GraphQL Error'
                _logger.error(
                    f"Shopify GraphQL Error encountered. Stopping pagination for current field group.{result}")
                if isinstance(error_details, dict) and error_details.get('extensions', {}).get(
                        'code') == 'MAX_COST_EXCEEDED':
                    cost = error_details['extensions'].get('cost')
                    max_cost = error_details['extensions'].get('maxCost')
                    _logger.error(
                        f"MAX_COST_EXCEEDED: Query cost {cost} exceeded limit {max_cost}. Query: {shopify_query}. Result: {result}")
                break
            orders_data = result.get('data', {}).get('orders', {})
            nodes = orders_data.get('nodes', [])
            page_info = orders_data.get('pageInfo', {})
            has_next_page = page_info.get('hasNextPage', False)
            _logger.info(f'Has next page: {has_next_page} and end cursor: {page_info.get("endCursor")}')
            end_cursor = page_info.get('endCursor')
            for order in nodes:
                order_id = order.get('id')
                if not order_id:
                    continue
                if order_id not in orders_by_id:
                    orders_by_id[order_id] = []
                orders_by_id[order_id].append(order)
        return orders_by_id

    def deep_merge_dicts(self, dicts):
        """
        Merge a list of dicts into one dict (deep merge).
        """
        def merge(a, b):
            for k, v in b.items():
                if k in a and isinstance(a[k], dict) and isinstance(v, Mapping):
                    merge(a[k], v)
                else:
                    a[k] = v
            return a
        result = {}
        for d in dicts:
            merge(result, d)
        return result

    @staticmethod
    def _extract_id_from_gid(gid: str) -> Optional[int]:
        """Extracts the numerical ID from a Shopify Global ID string."""
        if not isinstance(gid, str):
            return None
        match = re.search(r'\/(\d+)$', gid)
        return int(match.group(1)) if match else None

    # In OrderQueryHelper class
    def _flatten_money_set(self, key: str, value: Dict[str, Any]) -> Tuple[str, str, str]:
        """
        Flattens GraphQL MoneySet objects into the REST key/value pair.
        Conditionally selects shopMoney or presentmentMoney based on self.use_presentment.
        """
        # 1. Determine the REST-style key (snake_case, removing 'Set')
        new_key_rest = key.replace('Set', '')
        new_key_rest = re.sub(r'(?<!^)(?=[A-Z])', '_', new_key_rest).lower()

        # 2. Handle specific REST key names and duplicates
        if new_key_rest in ('totalprice', 'totaldiscounts'):
            new_key_rest = new_key_rest.replace('total', 'total_')
        if new_key_rest in ('original_unit_price', 'original_price'):
            new_key_rest = 'price'
        if new_key_rest == 'allocated_amount':
            new_key_rest = 'amount'  # Ensures the resulting key is 'amount'
        if self.use_presentment:
            amount_to_use = value['presentmentMoney']['amount']
        else:
            amount_to_use = value['shopMoney']['amount']
        set_key = new_key_rest + '_set'
        return new_key_rest, set_key, amount_to_use

    def _convert_graphql_to_rest_fields(self, data: Any) -> Any:
        """
        Recursively processes the GraphQL response to convert GIDs, flatten money sets,
        handle list nodes, and flatten line item specific nested fields for REST compatibility.
        @author: Gopal Chouhan @Emipro Technologies Pvt. Ltd on date 27/.
        """
        if isinstance(data, dict):
            new_data = {}
            # Assuming the class is named 'OrderQueryHelper' for static method calls
            # If the actual class name is different, replace 'OrderQueryHelper' below.
            OrderQueryHelper = self.__class__
            for key, value in data.items():
                # 1. Handle GraphQL 'nodes' list structure
                if key == 'nodes' and isinstance(value, list):
                    # Recursively convert all items in the list
                    return [self._convert_graphql_to_rest_fields(item) for item in value]

                # 2. Handle embedded list wrappers (e.g., lineItems: { nodes: [...] })
                if isinstance(value, dict) and 'nodes' in value:
                    # Convert the collection key from CamelCase (e.g., 'LineItems') to snake_case ('line_items')
                    new_key = re.sub(r'(?<!^)(?=[A-Z])', '_', key).lower()
                    # Recursively convert the 'nodes' list and assign it to the new snake_case key
                    new_data[new_key] = self._convert_graphql_to_rest_fields(value.get('nodes', []))
                    continue  # Skip the rest of the loop for this key

                # 3. Flatten MoneySet fields and map to REST names
                if key.endswith('Set') and isinstance(value, dict) and 'shopMoney' in value:
                    # Call _flatten_money_set as an instance method
                    new_key_rest, set_key, amount = self._flatten_money_set(key, value)

                    new_data[new_key_rest] = amount
                    new_data[set_key] = self._convert_graphql_to_rest_fields(value)

                    # Add currency field based on the conditional logic
                    if new_key_rest in ('amount', 'total_price', 'subtotal_price',
                                        'price'):  # Applies to transactions and core price fields
                        if self.use_presentment:
                            new_data['currency'] = value['presentmentMoney']['currencyCode']
                        else:
                            new_data['currency'] = value['shopMoney']['currencyCode']

                    continue

                # 4. Convert GIDs
                elif key == 'id' and isinstance(value, str) and value.startswith('gid://shopify/'):
                    # Convert GID to numeric ID
                    numeric_id = OrderQueryHelper._extract_id_from_gid(value)
                    new_data[key] = numeric_id
                    # Also keep the admin_graphql_api_id for full REST compatibility
                    new_data['admin_graphql_api_id'] = value

                # 5. Handle Line Item specific renames (originalTotalSet mapping)
                elif key == 'originalTotalSet' and isinstance(data, dict):
                    # Omit as it maps to 'price' which is handled by originalUnitPriceSet -> price
                    continue

                # 5.5. NEW: Flatten Line Item Nested Data (Product, Variant, Fulfillment Service)
                elif key == 'product' and isinstance(value, dict) and 'id' in value:
                    # Move product ID to top level as 'product_id'
                    new_data['product_id'] = OrderQueryHelper._extract_id_from_gid(value['id'])
                    # Don't continue here yet, in case 'product' has other fields you need to recurse over.

                elif key == 'variant' and isinstance(value, dict):
                    # Move variant ID, SKU, and Title to top level
                    new_data['variant_id'] = OrderQueryHelper._extract_id_from_gid(value.get('id'))
                    new_data['sku'] = value.get('sku')
                    new_data['variant_title'] = value.get('title')
                    # Do not recurse further on 'variant' to prevent unwanted keys from merging
                    continue

                elif key == 'fulfillment_service' and isinstance(value, dict) and 'service_name' in value:
                    # Flatten the fulfillment service name from the dictionary
                    new_data['fulfillment_service'] = value.get('service_name')
                    continue

                # 6. Generic recursion and key conversion (CamelCase to snake_case)
                else:
                    new_key = key
                    # Only convert to snake_case if it doesn't match an already defined REST field name
                    # and isn't a special-cased GraphQL field.
                    # 1. Update Exclusion List: Keep only admin_graphql_api_id and currencyCode
                    # We remove displayFinancialStatus and displayFulfillmentStatus to allow
                    # them to be converted to snake_case first.
                    if key not in ('admin_graphql_api_id', 'currencyCode'):

                        # Apply general CamelCase to snake_case conversion (e.g., displayFinancialStatus -> display_financial_status)
                        new_key = re.sub(r'(?<!^)(?=[A-Z])', '_', key).lower()

                        # Clean up specific common conversions (These lines remain helpful)
                        new_key = new_key.replace('code_v2', 'code')
                        new_key = new_key.replace('client_ip', 'browser_ip')
                        new_key = new_key.replace('legacy_resource_id', 'id')
                        if new_key == 'display_financial_status':
                            new_key = 'financial_status'
                        if new_key == 'display_fulfillment_status':
                            new_key = 'fulfillment_status'
                        if new_key == 'name' and isinstance(data, dict):
                            # We need to ensure this is the top-level 'name' field and not a nested one.
                            # This will create a key 'order_number' with the same value as 'name'.
                            new_data['order_number'] = value
                        if new_key in ('kind', 'status', 'source_name', 'financial_status', 'fulfillment_status') and \
                                isinstance(value, str):
                            value = value.lower()
                    new_data[new_key] = self._convert_graphql_to_rest_fields(value)
            return new_data
        elif isinstance(data, list):
            # Recurse through list items
            return [self._convert_graphql_to_rest_fields(item) for item in data]
        return data

