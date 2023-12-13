import unittest
from pyspark.sql import SparkSession
from PysparkRepo.src.Assignment_2.utils import *

class TestMaskCardNumber(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("TestMaskCardNumber").getOrCreate()

    def test_mask_card_number(self):
        test_cases = {
            "1234567891234567": "************4567",
            "5678912345671234": "************1234",
            "9123456712345678": "************5678",
            "1234567812341122": "************1122",
            "1234": "1234"
        }

        for card_number, expected_masked in test_cases.items():
            masked = mask_card_number(card_number)
            self.assertEqual(masked, expected_masked)

    def tearDown(self):
        self.spark.stop()

if __name__ == '__main__':
    unittest.main()
