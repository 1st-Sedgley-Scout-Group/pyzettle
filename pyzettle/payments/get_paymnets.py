import requests
from  pyzettle.authenticate import Authenticate
from dotenv import load_dotenv
import pandas as pd
from pyzettle.payments import drop_columns
from typing_extensions import Self
from pyzettle.payments.mapping import EVENT_MAP,CATEGORY_MAPPING, UNIFIED_PRODUCT_MAPPING

API_URL = "https://purchase.izettle.com/purchases/v2"
CASH_CARD_MAPPING = {
    'IZETTLE_CARD': 'card',
    'IZETTLE_CASH': 'cash',
    'GIFTCARD' : 'giftcard',
}

class GetPayments(Authenticate):
    def __init__(self: Self, client_id: str, api_key:str) -> None:
        self.api_url = API_URL
        self.data = pd.DataFrame()

        super().__init__(client_id, api_key)

        self._fetch_purchases_from_zettle()

    def _drop_columns(self: Self, list_name:str) -> Self:
        if list_name.lower() == "initial":
            list_name_ref = drop_columns.INITIAL
        elif list_name.lower() == "final":
            list_name_ref = drop_columns.FINAL
        else:
            raise ValueError(f"\"list_name\" must be \"initial\" or \"final\". Got {list_name}.")

        self.data = self.data.drop(columns=list_name_ref, errors='ignore')
        return self

    def _unpack_payments(self: Self) -> Self:

        if 'payments' not in self.data.columns:
            raise ValueError("\"Payments\" column is not in the DataFrame so cannot be completed")

        payments = self.data.explode('payments')
        payments = pd.json_normalize(payments['payments'])
        payments = payments.drop(columns=drop_columns.PAYMENTS)

        self.data = self.data.drop(columns=['payments']).reset_index(drop=True).join(payments.reset_index(drop=True))

        return self

    def _unpack_products(self: Self) -> Self:

        if 'products' not in self.data.columns:
            raise ValueError("\"products\" column is not in the DataFrame so cannot be completed")

        products = self.data.explode('products')

        products_norm = pd.json_normalize(products['products'])
        products_norm = products_norm.drop(columns=drop_columns.PRODUCTS)

        products = products.drop(columns=['products']).reset_index(drop=True)
        self.data = products.join(products_norm.reset_index(drop=True))

        return self

    def _format_zettle_payments(self: Self) -> Self:

        column_name_mapping = {
        "purchaseNumber" : "purchase_number",
        "customAmountSale" : "custom_amount_sale",
        "type" : "payment_type",
        "unitPrice" : "unit_price",
        "name" : "product",
        }

        self._drop_columns("initial")._unpack_payments()._unpack_products()._drop_columns("final")
        self.data['type'] = self.data['type'].replace(CASH_CARD_MAPPING)
        self.data['name'] = self.data['name'].replace('', "unknown")

        self.data = self.data.rename(columns=column_name_mapping)

        self.data["timestamp"] = pd.to_datetime(self.data["timestamp"])

        # In 2022 we had some payments of minus figures. This line makes them positive
        self.data = self.data.assign(quantity=self.data["quantity"].astype(int).abs())

        return self

    def _convert_pence_to_pounds(self: Self) -> Self:
        self.data['unit_price'] = self.data['unit_price']/100
        return self

    def _fetch_purchases_from_zettle(self: Self) -> Self:

        headers = {"Authorization": f"Bearer {self.access_token}"}

        params = {"descending": "true"}

        while True:
            response = requests.get(self.api_url, headers=headers, params=params)
            response.raise_for_status()  # Ensure we notice bad responses
            response_json = response.json()

            # Append the purchases data to the DataFrame
            purchases = pd.DataFrame(response_json.get('purchases', []))
            self.data = pd.concat([self.data, purchases], ignore_index=True)

            # Check if there are more pages
            last_purchase_hash = response_json.get('lastPurchaseHash')
            if not last_purchase_hash:
                break  # Exit loop if no more pages

            # Update params with the lastPurchaseHash for the next request
            params['lastPurchaseHash'] = last_purchase_hash

        self._format_zettle_payments()
        self._convert_pence_to_pounds()

        return self

    def normalise_quantities(self: Self) -> Self:

        # Use index.repeat to create multiple rows based on quantity
        self.data = self.data.loc[self.data.index.repeat(self.data["quantity"])].copy()

        # Set all quantities to 1
        self.data["quantity"] = 1

        # Reset index
        self.data = self.data.reset_index(drop=True)

        return self

    def filter_data(self: Self, week_days: list[str], products: list[str] = None) -> Self:

        if week_days is not None:
            self.data = self.data[self.data["timestamp"].dt.day_name().isin(week_days)]

        if products is not None:
            self.data = self.data[self.data["product"].isin(products)]

        return self

    def _add_event_information(self: Self) -> Self:
        # Iterate through events and map timestamps
        for event, event_info in EVENT_MAP.items():
            # Create a mask for timestamps within the event's date range
            event_mask = (self.data['timestamp'] >= event_info['start']) & \
                        (self.data['timestamp'] <= event_info['end'])

            # Update event columns where the mask is True
            self.data.loc[event_mask, 'event'] = event_info['label']

        self.data['event'] = self.data['event'].fillna("unknown")

        return self

    def _map_product_details(self: Self, new_column_name: str, mapping_dict: dict, current_value_column: str ="product") -> Self:

        # Add 'cat' column initially as a copy of 'product'
        self.data[new_column_name] = self.data[current_value_column]

        # Initialize an empty list to collect dataframes
        cat_data_list = []

        # Process known event mappings
        for event, event_products in mapping_dict.items():
            # Create a subset for the specific event
            subset = self.data[self.data['event'] == event].copy()

            # Replace categories for this subset
            # Use a dictionary mapping instead of .replace(value=)
            subset[new_column_name] = subset[new_column_name].map(event_products).fillna(subset[new_column_name])

            # Append to the list instead of using pd.concat in the loop
            cat_data_list.append(subset)

        # Handle 'unknown' event
        unknown_subset = self.data[self.data['event'] == 'unknown'].copy()
        unknown_subset[new_column_name] = 'none'
        cat_data_list.append(unknown_subset)

        # Concatenate all dataframes at once
        self.data = pd.concat(cat_data_list, ignore_index=True)

        return self

    def augment_data(self: Self) -> Self:

        self._add_event_information()
        self._map_product_details(new_column_name='category', mapping_dict=CATEGORY_MAPPING)
        self._map_product_details(new_column_name='unified_product', mapping_dict=UNIFIED_PRODUCT_MAPPING)
        self.data['year'] = self.data['timestamp'].dt.year

        return self

    def resample_and_group_data(self: Self, bin = '5min') -> Self:

        self.data = self.data.sort_values('timestamp', ascending=False)

        # Set timestamp as index
        self.data.set_index("timestamp", inplace=True)

        # Resample by hour, summing 'quantity', averaging 'unit_price', and keeping first non-null values for other columns
        self.data = self.data.resample(bin).agg(
            {
                "quantity": "sum",
                "unit_price": "sum",
            }
        )
        self.data = self.data.rename(columns={'unit_price' : 'revenue'}).reset_index()

        return self

    def cumulative_sum(self: Self, column_to_cumsum: str, new_column_name: str) -> Self:
        self.data[new_column_name] = self.data[column_to_cumsum].cumsum()
        return self

    def rolling_average(self: Self, column_to_average: str, new_column_name: str, window: int):
        self.data[new_column_name] = self.data[column_to_average].rolling(window=window).mean()
        return self
