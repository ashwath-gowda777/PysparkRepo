from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

## Function to mask the card number
def mask_card_number(card_number):
    if len(card_number) >= 4:
        return "*" * (len(card_number) - 4) + card_number[-4:]
    else:
        return card_number

## Register the UDF
def mask_card_udf():
    return udf(mask_card_number, StringType())
