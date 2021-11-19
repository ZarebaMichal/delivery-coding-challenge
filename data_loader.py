import argparse
import logging

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

    def getDataBatch(self, batch_size):
        """
        Generator - Get data from source on batches.

        :returns One list for each batch. Each of those is a list of
                 dictionaries with the defined rows.
        """
        # TODO: implement - this dummy implementation returns one batch of data
        yield [
            {
                "headline.main": "The main headline",
                "_id": "1234",
            }
        ]

    def getSchema(self):
        """
        Return the schema of the dataset
        :returns a List containing the names of the columns retrieved from the
        source
        """
        # TODO: Update the schema to reflect the item (row)
        schema = [
            "title",
            "body",
            "created_at",
            "id",
            "summary",
            "abstract",
            "keywords",
        ]

        return schema


if __name__ == "__main__":
    config = {
        "api_response_file": "api_response.json",
        "reference_data_file": "reference_data.xlsx"
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
