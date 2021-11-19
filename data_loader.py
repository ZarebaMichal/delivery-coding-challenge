import argparse
import json
import logging

import pandas as pd

"""
Skeleton for Squirro Delivery Hiring Coding Challenge
August 2021
"""


log = logging.getLogger(__name__)


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

    def _flatten_dict(self, data: dict, keystring: str = ""):
        """Sufficent flattening dict"""
        if type(data) == dict and len(data) != 0:
            keystring = keystring + "." if keystring else keystring
            for k in data:
                yield from self._flatten_dict(data[k], keystring + str(k))
        else:
            yield keystring, data

    def _combine_excel(self, list_of_articles: list):
        """Add excel values to all articles"""
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

        # Preprocessing data
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

        for article in list_of_articles:
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
                reference_id = None
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

        return list_of_articles

    def getDataBatch(self, batch_size):
        """
        Generator - Get data from source on batches.

        :returns One list for each batch. Each of those is a list of
                 dictionaries with the defined rows.
        """
        with open(self.args.api_response_file) as json_file:
            data = json.load(json_file)

        list_of_articles = [
            {k: v for k, v in self._flatten_dict(article)}
            for article in self._combine_excel(data["response"]["docs"])
        ]
        for i in range(0, len(list_of_articles), batch_size):
            yield list_of_articles[i : i + batch_size]

    def getSchema(self):
        """
        Return the schema of the dataset
        :returns a List containing the names of the columns retrieved from the
        source
        """
        example_result = self.getDataBatch(10)
        all_keys = set().union(*(d.keys() for batch in example_result for d in batch))
        return all_keys


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
