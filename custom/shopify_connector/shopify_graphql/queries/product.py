class ProductQueryHelper:
    """
    Helper for building Shopify GraphQL product queries.
    """

    DEFAULT_FIELDS = """       id
      title
      descriptionHtml
      vendor
      createdAt
      handle
      updatedAt
      publishedAt
      templateSuffix
      tags
      status
      variants(first: 100) {
        edges {
          node {
            id
            title
            price
            position
            compareAtPrice
            createdAt
            updatedAt
            taxable
            barcode
            sku
            inventoryPolicy
            selectedOptions {
              name
              value
            }
            inventoryItem {
              measurement {
                weight {
                  unit
                  value
                }
              }
              requiresShipping
              tracked
              legacyResourceId
            }
            image {
              id
              src
            }
          }
        }
      }
      options {
        values
        position
        name
        id
      }
          media(first: 100) {
      edges {
        node {
          ... on MediaImage {
            id
            image {
              url
            }
          }
        }
      }
    }
  """

    @staticmethod
    def build_product_query(
        status, import_based_on, from_date, to_date, fields=None, after_cursor=None
    ):
        query_fields = fields or ProductQueryHelper.DEFAULT_FIELDS
        filter_parts = []
        if status:
            filter_parts.append(f"status:{status}")
        if import_based_on == "create_date" and from_date and to_date:
            filter_parts.append(f"created_at:>='{from_date}'")
            filter_parts.append(f"created_at:<='{to_date}'")
        elif import_based_on == "update_date" and from_date and to_date:
            filter_parts.append(f"updated_at:>='{from_date}'")
            filter_parts.append(f"updated_at:<='{to_date}'")
        filter_str = " ".join(filter_parts)
        after = f', after: "{after_cursor}"' if after_cursor else ""
        query = f"""
        {{
          products(first: 250, query: "{filter_str}"{after}) {{
              nodes {{
                {query_fields}
            }}
            pageInfo {{
              hasNextPage endCursor
            }}
          }}
        }}
        """
        return query
