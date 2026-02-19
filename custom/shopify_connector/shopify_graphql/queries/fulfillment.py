class FulfillmentQueryHelper:
    def __init__(self, client):
        self.client = client

    def get_fulfillment(self, fulfillment_id, fields=None):
        fields = fields or "id status trackingInfo"
        query = f'''
        {{
          fulfillment(id: "gid://shopify/Fulfillment/{fulfillment_id}") {{
            {fields}
          }}
        }}
        '''
        return self.client.execute(query)
