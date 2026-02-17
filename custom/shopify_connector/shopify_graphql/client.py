import requests
import logging
import time

_logger = logging.getLogger("Shopify GraphQL Client")


class GraphQLObject:
    """
    Generic object wrapper for GraphQL response dicts.
    Provides attribute access, .to_dict(), and .get() methods.
    """

    def __init__(self, data):
        self._data = data

    def to_dict(self):
        return self._data

    def get(self, key, default=None):
        return self._data.get(key, default)

    def __getattr__(self, key):
        try:
            return self._data[key]
        except KeyError:
            raise AttributeError(key)


class ShopifyGraphQLClient:
    def __init__(self, access_token, shop_url):
        self.access_token = access_token
        self.shop_url = shop_url.rstrip('/')
        self.endpoint = f"{self.shop_url}/admin/api/2026-01/graphql.json"
        self.MAX_RETRIES = 3  # Max retries for a single API execution
        self.graphql_object = GraphQLObject

    def execute(self, query, variables=None):
        headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": self.access_token,
        }
        payload = {"query": query}
        response = []
        if variables:
            payload["variables"] = variables
        for attempt in range(self.MAX_RETRIES):
            try:
                # Execute the API call
                response = requests.post(self.endpoint, json=payload, headers=headers)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.ConnectionError as e:
                # Catch network specific errors (like Errno 101)
                if attempt < self.MAX_RETRIES - 1:
                    wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s...
                    _logger.warning(
                        f"Shopify network connection failed (Attempt {attempt + 1}/{self.MAX_RETRIES}). Retrying after {wait_time}s. Error: {e.args[0]}"
                    )
                    time.sleep(wait_time)
                    continue  # Continue to the next attempt
                else:
                    _logger.error(f"Shopify network connection failed after {self.MAX_RETRIES} attempts. Query failed.")
                    raise e
            except requests.exceptions.HTTPError as e:
                _logger.error(f"Shopify HTTP Error {e.response.status_code}. Query failed.")
                raise e
            except Exception as e:
                # Catch other unknown exceptions
                _logger.error(f"Unexpected error during Shopify API execution: {e}.")
                raise e
        return response

    def bulk_operation(self, query):
        bulk_query = """
        mutation {
          bulkOperationRunQuery(
            query: """ + query.replace('"', '\"') + """
          ) {
            bulkOperation {
              id
              status
            }
            userErrors {
              field
              message
            }
          }
        }
        """
        return self.execute(bulk_query)

    @staticmethod
    def build_query(query_name=None, object_name=None, connection_name=None, fields=None, filters=None, first=250,
                    after=None, extra_connection_args=None, use_nodes=False):
        """
        Build a generic GraphQL query string.
        :param query_name: Name of the outer query (e.g., 'MyQuery'). Optional.
        :param object_name: Root object (e.g., 'shopifyPaymentsAccount'). Optional for root-level connections.
        :param connection_name: Connection name under the object or at root (e.g., 'orders').
        :param fields: Fields string or nested structure
        :param filters: dict, list, or string of filter strings (e.g., {'status': 'paid'}, ["issued_at:>='2025-01-01'", ...], or "status:paid issued_at:>='2025-01-01'")
        :param first: Number of records per page
        :param after: Pagination cursor
        :param extra_connection_args: Additional arguments for the connection (e.g., 'sortKey: UPDATED_AT')
        :param use_nodes: If True, use 'nodes' instead of 'edges' for the connection (for root-level queries)
        :return: GraphQL query string
        """
        # Handle filters as dict, list, or string
        if isinstance(filters, dict):
            filter_parts = [f"{k}:{v}" for k, v in filters.items()]
        elif isinstance(filters, list):
            filter_parts = filters
        elif isinstance(filters, str):
            filter_parts = [filters]
        else:
            filter_parts = []
        filter_str = ' '.join(filter_parts)
        after_str = f', after: "{after}"' if after else ''
        extra_args = f', {extra_connection_args}' if extra_connection_args else ''
        # Compose the connection query
        connection_args = f"first: {first}{after_str}, query: \"{filter_str}\"{extra_args}"
        if use_nodes:
            connection_query = f"""
            {connection_name}({connection_args}) {{
                pageInfo {{ endCursor hasNextPage }}
                nodes {{
                    {fields}
                }}
            }}"""
        else:
            connection_query = f"""
            {connection_name}({connection_args}) {{
                edges {{
                    node {{
                        {fields}
                    }}
                }}
                pageInfo {{
                    endCursor
                    hasNextPage
                }}
            }}"""
        # Compose the object block if object_name is provided
        if object_name:
            object_block = f"{object_name} {{\n{connection_query}\n}}"
        else:
            object_block = connection_query
        # Compose the full query, with or without query_name
        if query_name:
            query = f"""
            query {query_name} {{
                {object_block}
            }}
            """
        else:
            query = f"""
            {{
                {object_block}
            }}
            """
        return query

    @staticmethod
    def parse_connection_response(response, object_name=None, connection_name=None, use_nodes=False):
        """
        Parse a Shopify GraphQL connection response.
        :param response: Raw response dict from Shopify
        :param object_name: Root object (e.g., 'shopifyPaymentsAccount'), or None for root-level
        :param connection_name: Connection name (e.g., 'orders', 'balanceTransactions')
        :param use_nodes: If True, extract from 'nodes', else from 'edges'
        :return: (has_next_page, end_cursor, data_list)
        :raises: Exception if errors are present or structure is invalid
        """
        if 'errors' in response:
            raise Exception(f"Shopify GraphQL error: {response['errors']}")
        if not response or 'data' not in response:
            raise Exception(f"Invalid or empty response from Shopify GraphQL API.\nResponse: {response}")
        data = response['data']
        if object_name:
            data = data.get(object_name, {})
        conn = data.get(connection_name, {})
        page_info = conn.get('pageInfo', {})
        has_next = page_info.get('hasNextPage', False)
        end_cursor = page_info.get('endCursor')
        if use_nodes:
            data_list = conn.get('nodes', [])
        else:
            data_list = [edge.get('node', {}) for edge in conn.get('edges', [])]
        return has_next, end_cursor, data_list

    def fetch_all_connection_data(self, query_name=None, object_name=None, connection_name=None, fields=None,
                                  filters=None, first=250, extra_connection_args=None, use_nodes=False,
                                  no_pagination=False):
        """
        Fetch all data from a Shopify GraphQL connection, handling pagination automatically unless no_pagination is True.
        :param query_name: Name of the outer query (optional)
        :param object_name: Root object (optional for root-level)
        :param connection_name: Connection name (required)
        :param fields: Fields string or nested structure
        :param filters: dict or list of filter strings
        :param first: Number of records per page
        :param extra_connection_args: Additional arguments for the connection
        :param use_nodes: If True, extract from 'nodes', else from 'edges'
        :param no_pagination: If True, fetch only the first page and return pagination info
        :return: (data_list, has_next_page, end_cursor)
        """
        all_data = []
        after = None
        has_next = False
        end_cursor = None
        while True:
            query = self.build_query(
                query_name=query_name,
                object_name=object_name,
                connection_name=connection_name,
                fields=fields,
                filters=filters,
                first=first,
                after=after,
                extra_connection_args=extra_connection_args,
                use_nodes=use_nodes
            )
            result = self.execute(query)
            has_next, end_cursor, data_list = self.parse_connection_response(
                result,
                object_name=object_name,
                connection_name=connection_name,
                use_nodes=use_nodes
            )
            all_data.extend(data_list)
            if no_pagination:
                break
            if not has_next:
                break
            after = end_cursor
        return all_data, has_next, end_cursor
