import unittest
from pyspark.sql import SparkSession
from PysparkRepo.src.Assignment_5.utils import *

class TestUtils(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("TestUtils").getOrCreate()
        self.employee_schema = define_employee_schema()
        self.department_schema = define_department_schema()
        self.country_schema = define_country_schema()

        self.employee_df, self.department_df, self.country_df = create_dataframes(
            self.spark, self.employee_schema, self.department_schema, self.country_schema
        )

        # Ensure add_bonus_column is called before reorder_columns
        self.employee_df = add_bonus_column(self.employee_df)

    def tearDown(self):
        self.spark.stop()

    def test_avg_salary(self):
        avg_salary_department = calculate_avg_salary(self.employee_df)
        self.assertIsNotNone(avg_salary_department)

    def test_employees_start_with_m(self):
        employees_start_with_m = find_employees_start_with_m(self.employee_df)
        self.assertIsNotNone(employees_start_with_m)

    def test_add_bonus_column(self):
        # Already called in setUp, check if bonus column exists
        self.assertIn('bonus', self.employee_df.columns)

    def test_reorder_columns(self):
        # Reorder columns after adding bonus column
        reordered_employee_df = reorder_columns(self.employee_df)
        self.assertIsNotNone(reordered_employee_df)
        self.assertIn('bonus', reordered_employee_df.columns)  # Ensure 'bonus' column is in the reordered DataFrame

    def test_perform_joins(self):
        inner_join, left_join, right_join = perform_joins(self.employee_df, self.department_df)
        self.assertIsNotNone(inner_join)
        self.assertIsNotNone(left_join)
        self.assertIsNotNone(right_join)

    def test_derive_new_dataframe(self):
        new_employee_df = derive_new_dataframe(self.employee_df, self.country_df)
        self.assertIsNotNone(new_employee_df)

    def test_convert_columns_and_add_load_date(self):
        final_df = convert_columns_and_add_load_date(self.employee_df)
        self.assertIsNotNone(final_df)

if __name__ == "__main__":
    unittest.main()
