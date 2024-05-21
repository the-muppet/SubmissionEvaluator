This script is designed to evaluate TCGplayer SYP submissions.

**1. Script Functionality**

The script performs the following key functions:

- **Data Loading:** Loads CSV files containing information about the pullsheet, pull order, catalog, and submission.
- **Data Cleaning:** Validates and cleans the data based on predefined rules to ensure data accuracy and consistency.
- **Data Merging:** Merges the DataFrames based on shared columns to combine information from different sources.
- **Metric Calculation:** Calculates various metrics to assess the quality and feasibility of the submission, including:
    - **Average Card Value (ACV):** Represents the average value of each card in the submission.
    - **Match Rate:** Measures the percentage of submitted cards that are found on the pullsheet and are valid based on maximum quantities allowed.
    - **Pullsheet Missing Rate:** Calculates the percentage of submitted cards that are not found on the pullsheet.
    - **Catalog On Pullsheet Rate:** Calculates the percentage of catalog items that are present on the pullsheet.
    - **Total Rejected Quantity:** Calculates the number of cards rejected due to exceeding the maximum quantity allowed.
- **Submission Evaluation:** Determines the status of the submission (accepted or rejected) based on predefined criteria, considering the ACV and total quantity.
- **Curation (Optional):** Attempts to adjust the submission by removing items to potentially meet acceptance criteria if the initial status is rejected.
- **Report Generation:** Generates a summary report containing the calculated metrics and the status of the submission.

**2. Variables and Representations:**

- **`dataframe`:** A pandas DataFrame representing the submission data, containing information about each submitted card.
- **`threshold`:** A user-defined float value representing the minimum ACV required for the submission to be accepted.
- **`_acv`:** A float representing the calculated average card value of the submission.
- **`_match_rate`:** A float representing the match rate, which indicates the percentage of submitted cards found on the pullsheet and valid based on maximum quantities.
- **`_total_value`:** A float representing the total value of all cards in the submission.
- **`_total_quantity`:** An integer representing the total quantity of cards in the submission.
- **`_total_adjusted_qty`:** An integer representing the total quantity of cards that can be accepted after considering the maximum quantities allowed per card on the pullsheet.
- **`_status`:** A boolean value representing the status of the submission, with `True` for accepted and `False` for rejected.
- **`_pullsheet_missing_rate`:** A float representing the percentage of submitted cards not found on the pullsheet.
- **`_catalog_on_pullsheet_rate`:** A float representing the percentage of catalog items found on the pullsheet.
- **`_total_rejected_quantity`:** An integer representing the total number of cards rejected due to exceeding the maximum quantity allowed.

**3. Mathematical Calculations:**

**3.1. Average Card Value (ACV):**

- **Formula:** ACV = (Total Value) / (Total Quantity)
- **Calculation:**
    - `total_value` is the sum of the value of all cards in the submission.
    - `total_quantity` is the total number of cards in the submission.
    - The ACV is calculated by dividing the total value by the total quantity.

**3.2. Match Rate:**

- **Formula:** Match Rate = (Total Adjusted Quantity) / (Total Quantity) * 100%
- **Calculation:**
    - `total_adjusted_qty` represents the total quantity of cards that can be accepted based on the pullsheet's maximum quantities.
    - `total_quantity` is the total number of cards in the submission.
    - The match rate is calculated by dividing the total adjusted quantity by the total quantity and multiplying by 100%.

**3.3. Pullsheet Missing Rate:**

- **Formula:** Pullsheet Missing Rate = (Missing Quantity) / (Total Quantity) * 100%
- **Calculation:**
    - `missing_quantity` is the number of submitted cards that are not found on the pullsheet.
    - `total_quantity` is the total number of cards in the submission.
    - The pullsheet missing rate is calculated by dividing the missing quantity by the total quantity and multiplying by 100%.

**3.4. Catalog On Pullsheet Rate:**

- **Formula:** Catalog On Pullsheet Rate = (Items On Pullsheet) / (Total Catalog Items) * 100%
- **Calculation:**
    - `items_on_pullsheet` is the number of unique card IDs present in both the catalog and the pullsheet.
    - `total_catalog_items` is the total number of unique card IDs in the catalog.
    - The catalog on pullsheet rate is calculated by dividing the number of items on the pullsheet by the total catalog items and multiplying by 100%.

**3.5. Total Rejected Quantity:**

- **Formula:** Total Rejected Quantity = Total Quantity - Total Adjusted Quantity
- **Calculation:**
    - `total_quantity` is the total number of cards in the submission.
    - `total_adjusted_qty` is the total quantity of cards that can be accepted based on the pullsheet's maximum quantities.
    - The total rejected quantity is calculated by subtracting the total adjusted quantity from the total quantity.

**4. Submission Evaluation:**

- The submission is evaluated based on two criteria:
    - **Minimum Quantity:** The total quantity of cards in the submission must be at least 500.
    - **ACV Threshold:** The ACV of the submission must be greater than or equal to the predefined threshold value.
- If both criteria are met, the submission is considered accepted; otherwise, it is rejected.
