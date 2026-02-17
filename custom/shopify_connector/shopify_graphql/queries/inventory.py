class InventoryQueryHelper:
    def __init__(self, client):
        self.client = client

    def get_inventory_item(self, inventory_item_id, fields=None):
        fields = fields or "id sku tracked"
        query = f'''
        {{
          inventoryItem(id: "gid://shopify/InventoryItem/{inventory_item_id}") {{
            {fields}
          }}
        }}
        '''
        return self.client.execute(query)

    def get_inventory_levels(self, location_id, after=None, first=20):
        """
        Fetch inventoryItems nodes containing inventoryLevel for a given location.
        Returns the raw GraphQL response (dict) so callers can inspect data and pageInfo.
        :param location_id: numeric id string (not gid) or the gid suffix used in the gid string
        :param after: cursor for pagination (string) or None
        :param first: number of items to request
        """
        after_clause = f', after: "{after}"' if after else ''
        query = f'''
        query ShopName {{ inventoryItems(first: {first}{after_clause}) {{
          nodes {{
            inventoryLevel(locationId: "gid://shopify/Location/{location_id}") {{
              quantities(names: ["available"]) {{
                id
                name
                quantity
              }}
            }}
          }}
          pageInfo {{
            endCursor
            hasNextPage
          }}
        }} }}
        '''
        return self.client.execute(query)

    def get_all_inventory_levels(self, location_id, first=20):
        """
        Fetch all inventoryItems for a given location by following pagination and
        return a combined response dict in the same shape as a single-page response.
        The returned dict will contain data.inventoryItems.nodes as the combined list
        and data.inventoryItems.pageInfo corresponding to the last page.
        """
        # First page
        response = self.get_inventory_levels(location_id, after=None, first=first)
        if not response:
            return response

        # Safeguard: extract pageInfo and nodes
        data = response.get('data', {})
        inv_items = data.get('inventoryItems') if data else None
        if not inv_items:
            return response

        combined_nodes = inv_items.get('nodes', []) or []
        page_info = inv_items.get('pageInfo') or {}

        # Iterate following pages
        while page_info.get('hasNextPage'):
            end_cursor = page_info.get('endCursor')
            next_resp = self.get_inventory_levels(location_id, after=end_cursor, first=first)
            if not next_resp:
                break
            next_data = next_resp.get('data', {})
            next_items = next_data.get('inventoryItems') if next_data else None
            if not next_items:
                break
            next_nodes = next_items.get('nodes', []) or []
            if next_nodes:
                combined_nodes.extend(next_nodes)
            page_info = next_items.get('pageInfo') or {}

        # Build combined response similar to original
        combined = {
            'data': {
                'inventoryItems': {
                    'nodes': combined_nodes,
                    'pageInfo': page_info,
                }
            }
        }
        return combined

    def set_inventory_quantities(self, quantities, reason='correction', name='available', ignore_compare=True):
        """
        Execute the inventorySetQuantities mutation.

        :param quantities: either a preformatted GraphQL array string (e.g. "[{...}, {...}]")
                           or a list of dicts with keys: inventory_item_id, location_id, quantity
                           where ids are plain numeric ids (not GID). Example:
                           [{'inventory_item_id': '123', 'location_id': '456', 'quantity': 10}, ...]
        :param reason: reason string for mutation (default 'correction')
        :param name: which quantity name to set (default 'available')
        :param ignore_compare: bool flag for ignoreCompareQuantity
        :return: parsed JSON response (dict) from the GraphQL API
        """
        # Prepare quantities string
        if isinstance(quantities, str):
            quantities_str = quantities
        else:
            parts = []
            for q in quantities:
                inv_id = q.get('inventory_item_id')
                loc_id = q.get('location_id')
                qty = q.get('quantity')
                part = f'{{inventoryItemId: "gid://shopify/InventoryItem/{inv_id}", locationId: "gid://shopify/Location/{loc_id}", quantity: {int(qty)}}}'
                parts.append(part)
            quantities_str = "[" + ", ".join(parts) + "]"

        ignore_flag = 'true' if ignore_compare else 'false'
        mutation = f'''
        mutation InventorySetQuantities {{
          inventorySetQuantities(
            input: {{
              reason: "{reason}",
              name: "{name}",
              quantities: {quantities_str},
              ignoreCompareQuantity: {ignore_flag}
            }}
          ) {{
            userErrors {{ code field message }}
          }}
        }}
        '''

        return self.client.execute(mutation)

    def build_quantities_from_queue_lines(self, queue_lines):
        """
        Build a list of quantity dicts suitable for set_inventory_quantities from
        export queue line records.

        :param queue_lines: iterable of records (expected to have inventory_item_id, location_id, quantity)
        :return: list of dicts: [{'inventory_item_id': '...', 'location_id': '...', 'quantity': 10}, ...]
        """
        quantities = []
        for line in queue_lines:
            try:
                inv_id = line.inventory_item_id
                loc_id = line.location_id
                qty = int(line.quantity or 0)
            except Exception:
                # Fallback: skip malformed lines
                continue
            quantities.append({'inventory_item_id': inv_id, 'location_id': loc_id, 'quantity': qty})
        return quantities
