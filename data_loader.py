import argparse
import collections
import json
import logging
from typing import Any, Deque, Dict, List, Tuple, Union

import pandas as pd
from pandas import DataFrame

"""
Skeleton for Squirro Delivery Hiring Coding Challenge
August 2021
"""


log = logging.getLogger(__name__)

JSON_VALUE = Union[str, int, float, bool, None, List[Any], Dict[str, Any]]
JSON_TYPE = Dict[str, JSON_VALUE]
QUEUE = Deque[Tuple[str, JSON_VALUE]]


class NYTimesSource(object):
    """
    A data loader plugin for the NY Times Data.
    """

    def __init__(self):
        pass

    def connect(self, inc_column=None, max_inc_value=None):
        # Ignore this method
        log.debug("Incremental Column: %r", inc_column)
        log.debug("Incremental Last Value: %r", max_inc_value)

    def disconnect(self):
        """Disconnect from the source."""
        # Nothing to do
        pass

    def _flatten_iteration(self, input_dict: JSON_TYPE) -> JSON_TYPE:
        """Flatten dictionary with '.' separator"""
        queue: QUEUE = collections.deque([("", input_dict)])
        output_dict: JSON_TYPE = {}
        while queue:
            key, value = queue.popleft()
            if isinstance(value, dict):
                prefix = f"{key}." if key else ""
                queue.extend((f"{prefix}{k}", v) for k, v in value.items())
            else:
                output_dict[key] = value
        return output_dict

    def _get_excel_sheets(self):
        """Create dataframes for each sheet in excel."""
        review_status = pd.read_excel(
            self.args.reference_data_file,
            sheet_name="review_status",
            header=2,
            usecols="B:E",
            dtype=str,
        )
        date_completed = pd.read_excel(
            self.args.reference_data_file, sheet_name="date_completed"
        )

        return review_status, date_completed

    def _preprocess_data(
        self, review_status: DataFrame, date_completed: DataFrame
    ) -> Tuple[DataFrame, DataFrame]:
        """Preprocess DataFrames"""
        review_status.columns = review_status.columns.str.replace(" ", "_").str.lower()
        date_completed.columns = date_completed.columns.str.replace(
            " ", "_"
        ).str.lower()
        review_status["article_id"] = review_status["article_id"].apply(
            lambda x: x.strip()
        )
        date_completed["reference_id"] = date_completed["reference_id"].values.astype(
            str
        )

        return review_status, date_completed

    def _combine_excel(self, article: dict):
        """Add excel values to article"""
        review_status, date_completed = self._preprocess_data(*self._get_excel_sheets())

        excel_dict = {}
        article_status = review_status.loc[
            review_status["article_id"] == article["_id"]
        ]
        if len(article_status) > 1:
            excel_dict["status.duplicates"] = len(article_status)
            excel_dict["status.duplicates.reference_id"] = article_status[
                "reference_id"
            ].tolist()

        if article_status.empty:
            excel_dict.update(article_status.to_dict())
            reference_id = []
        else:
            excel_dict.update(article_status.tail(1).to_dict("records")[0])
            reference_id = article_status["reference_id"].values

        article_date = date_completed.loc[
            date_completed["reference_id"].isin(reference_id)
        ]

        if article_date.empty:
            excel_dict.update(article_date.to_dict())
        else:
            excel_dict.update(article_date.tail(1).to_dict("records")[0])

        article.update(excel_dict)

        return article

    def _get_articles(self) -> list:
        """
        Extract articles from json response.

        :returns List of articles.
        """
        with open(self.args.api_response_file) as json_file:
            data = json.load(json_file)

        return data["response"]["docs"]

    def getDataBatch(self, batch_size):
        """
        Generator - Get data from source on batches.

        :returns One list for each batch. Each of those is a list of
                 dictionaries with the defined rows.
        """
        batch = []
        for article in self._get_articles():
            flatten_article = self._flatten_iteration(article)
            combined_article = self._combine_excel(flatten_article)
            batch.append(combined_article)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def getSchema(self):
        """
        Return the schema of the dataset
        :returns a List containing the names of the columns retrieved from the
        source
        """
        batch = next(self.getDataBatch(10), ())
        fields = set()
        for entry in batch:
            fields |= entry.keys()
        return sorted(fields)


if __name__ == "__main__":
    config = {
        "api_response_file": "api_response.json",
        "reference_data_file": "reference_data.xlsx",
    }
    source = NYTimesSource()

    # This looks like an argparse dependency - but the Namespace class is just
    # a simple way to create an object holding attributes.
    source.args = argparse.Namespace(**config)

    for idx, batch in enumerate(source.getDataBatch(2)):
        print(f"{idx} Batch of {len(batch)} items")
        for item in batch:
            # Hint item["status"] and item.get("date_completed") should come from the
            # excel file
            print(f"{item['_id']} - {item['headline.main']}")
            print(f" --> {item['status']} - {item.get('date_completed')}")
