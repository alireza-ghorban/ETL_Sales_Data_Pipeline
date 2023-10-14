# Metrics Document

This document provides a detailed explanation of the metrics used in the ETL Data Pipeline Project, specifically for the data transformation process. The data, which comes from daily sales and inventory transactional tables, is processed and transformed to generate insights grouped by each week, store, and product.

## Metrics Definitions

### 1. Total Sales Quantity of a Product
- **Formula**: `Sum(sales_qty)`
- **Description**: The cumulative sales quantity of a product for a given week, store, and product.

### 2. Total Sales Amount of a Product
- **Formula**: `Sum(sales_amt)`
- **Description**: The total sales amount for a product, aggregated over a specified week for each store and product.

### 3. Average Sales Price
- **Formula**: `Sum(sales_amt)/Sum(sales_qty)`
- **Description**: The average price at which a product was sold during the week.

### 4. Stock Level by the End of the Week
- **Description**: The `inventory_on_hand_qty` at the end day of the week.
- **Significance**: Reflects the quantity of products available in stock at the end of the week.

### 5. Inventory on Order Level by the End of the Week
- **Description**: The `ordered_inventory_qty` at the end day of the week.

### 6. Total Cost of the Week
- **Formula**: `Sum(sales_cost)`
- **Description**: The total cost associated with the products sold during the week.

### 7. Percentage of Store In-Stock
- **Formula**: (Number of times `out_of_stock` occurred in a week) / 7 
- **Description**: The ratio of days a product was out of stock to the total days in a week.

### 8. Total Low Stock Impact
- **Formula**: `sum(out_of_stock_flg + Low_Stock_flg)`
- **Description**: The combined effect of products being out of stock or having low stock during the week.

### 9. Potential Low Stock Impact
- **Formula**: If `Low_Stock_Flg = TRUE` then `SUM(sales_amt - stock_on_hand_amt)`
- **Description**: The potential sales impact if a product has low stock.

### 10. No Stock Impact
- **Formula**: If `out_of_stock_flg=true`, then `sum(sales_amt)`
- **Description**: The potential sales lost due to products being completely out of stock.

### 11. Low Stock Instances
- **Description**: Number of times `Low_Stock_Flg` is true in a week.

### 12. No Stock Instances
- **Description**: Number of times `out_of_Stock_Flg` is true in a week.

### 13. Weeks the On Hand Stock Can Supply
- **Formula**: `(inventory_on_hand_qty at the end of the week) / sum(sales_qty)`
- **Description**: The estimated number of weeks the current on-hand stock can fulfill the sales demand.

**Note**: 
- `Low Stock_flg` is flagged as `1` if the day's `inventory_on_hand_qty` is less than the `sales_qty`. Otherwise, it is flagged as `0`.
