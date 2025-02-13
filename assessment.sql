CREATE TABLE cleaned_sales_data AS  
SELECT 
    TransactionID, 
    CustomerID, 
    CAST(TransactionDate AS DATE) AS TransactionDate,  -- Convert to DATE format
    CAST(TransactionAmount AS DECIMAL(10,2)) AS TransactionAmount, 
    COALESCE(PaymentMethod, 'Unknown') AS PaymentMethod,  -- Fill missing values
    Quantity, 
    CAST(DiscountPercent AS DECIMAL(5,2)) AS DiscountPercent,  
    COALESCE(City, 'Unknown') AS City,  
    COALESCE(StoreType, 'Unknown') AS StoreType,  
    CAST(CustomerAge AS INT) AS CustomerAge, 
    CASE 
        WHEN CustomerAge IS NULL THEN (SELECT ROUND(AVG(CustomerAge), 0) FROM sales_data WHERE CustomerAge IS NOT NULL) 
        ELSE CustomerAge 
    END AS ImputedCustomerAge,  -- Impute missing CustomerAge with median
    COALESCE(CustomerGender, 'Unknown') AS CustomerGender,  
    COALESCE(LoyaltyPoints, 0) AS LoyaltyPoints,  -- Replace NULL loyalty points with 0
    COALESCE(ProductName, 'Unknown') AS ProductName,  
    COALESCE(Region, (SELECT Region FROM sales_data WHERE City IS NOT NULL LIMIT 1)) AS Region,  
    CASE 
        WHEN Returned = 'Yes' THEN 1 ELSE 0 
    END AS ReturnFlag,  -- Convert Returned column to binary
    COALESCE(FeedbackScore, 0) AS FeedbackScore,  
    CAST(ShippingCost AS DECIMAL(10,2)) AS ShippingCost,  
    CAST(DeliveryTimeDays AS INT) AS DeliveryTimeDays,  
    CAST(IsPromotional AS INT) AS IsPromotional  
FROM sales_data;

DELETE FROM cleaned_sales_data
WHERE TransactionID IN (
    SELECT TransactionID
    FROM (
        SELECT TransactionID, ROW_NUMBER() OVER (PARTITION BY TransactionID ORDER BY TransactionDate) AS row_num
        FROM cleaned_sales_data
    ) t
    WHERE row_num > 1
);

-- Remove outliers in TransactionAmount using IQR
DELETE FROM cleaned_sales_data
WHERE TransactionAmount < (
    SELECT PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY TransactionAmount) - 
    1.5 * (PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY TransactionAmount) - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY TransactionAmount))
) 
OR TransactionAmount > (
    SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY TransactionAmount) + 
    1.5 * (PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY TransactionAmount) - PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY TransactionAmount))
);

-- Create Net Revenue Column (Revenue after discount)
ALTER TABLE cleaned_sales_data ADD COLUMN NetRevenue DECIMAL(10,2);
UPDATE cleaned_sales_data
SET NetRevenue = TransactionAmount - (TransactionAmount * DiscountPercent / 100);

-- Define High-Value Orders (Above 90th Percentile)
ALTER TABLE cleaned_sales_data ADD COLUMN HighValueOrder INT;
UPDATE cleaned_sales_data
SET HighValueOrder = 
    CASE WHEN TransactionAmount >= (SELECT PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY TransactionAmount) FROM cleaned_sales_data) 
    THEN 1 ELSE 0 END;

-- Customer Segmentation Based on Purchase Frequency
ALTER TABLE cleaned_sales_data ADD COLUMN CustomerSegment VARCHAR(50);
UPDATE cleaned_sales_data
SET CustomerSegment = 
    CASE 
        WHEN CustomerID IN (SELECT CustomerID FROM cleaned_sales_data GROUP BY CustomerID HAVING COUNT(TransactionID) >= 10) THEN 'Loyal'
        WHEN CustomerID IN (SELECT CustomerID FROM cleaned_sales_data GROUP BY CustomerID HAVING COUNT(TransactionID) BETWEEN 3 AND 9) THEN 'Regular'
        ELSE 'One-Time'
    END;

-- Customer Lifetime Value (CLV)
ALTER TABLE cleaned_sales_data ADD COLUMN CustomerLifetimeValue DECIMAL(10,2);
UPDATE cleaned_sales_data
SET CustomerLifetimeValue = 
    (SELECT SUM(TransactionAmount) FROM cleaned_sales_data WHERE cleaned_sales_data.CustomerID = sales_data.CustomerID);

-- Discount Impact (Flag for >20% Discount)
ALTER TABLE cleaned_sales_data ADD COLUMN HighDiscountFlag INT;
UPDATE cleaned_sales_data
SET HighDiscountFlag = CASE WHEN DiscountPercent >= 20 THEN 1 ELSE 0 END;

-- Ensure all NULL values are properly handled
UPDATE cleaned_sales_data
SET FeedbackScore = COALESCE(FeedbackScore, 0),
    ShippingCost = COALESCE(ShippingCost, 0),
    DeliveryTimeDays = COALESCE(DeliveryTimeDays, 0),
    LoyaltyPoints = COALESCE(LoyaltyPoints, 0);

-- Total Revenue, cleaned_sales_data, and Average Order Value (AOV)
SELECT 
    SUM(TransactionAmount) AS Total_Revenue, 
    COUNT(TransactionID) AS Total_cleaned_sales_data, 
    ROUND(SUM(TransactionAmount) / COUNT(TransactionID), 2) AS Avg_Order_Value,
    ROUND(AVG(DiscountPercent), 2) AS Avg_Discount_Applied
FROM cleaned_sales_data;

-- Most Popular Payment Method
SELECT PaymentMethod, COUNT(*) AS Payment_Count
FROM cleaned_sales_data
GROUP BY PaymentMethod
ORDER BY Payment_Count DESC
LIMIT 1;

-- Most Frequent Store Type
SELECT StoreType, COUNT(*) AS Store_Count
FROM cleaned_sales_data
GROUP BY StoreType
ORDER BY Store_Count DESC
LIMIT 1;

-- Top 5 Revenue Generating Cities
SELECT City, SUM(TransactionAmount) AS Total_Sales
FROM cleaned_sales_data
GROUP BY City
ORDER BY Total_Sales DESC
LIMIT 5;

-- Top 5 Best-Selling Products
SELECT ProductName, SUM(Quantity) AS Total_Units_Sold
FROM cleaned_sales_data
GROUP BY ProductName
ORDER BY Total_Units_Sold DESC
LIMIT 5;

-- Sales Trend Over Time (Daily)
SELECT TransactionDate, SUM(TransactionAmount) AS Daily_Sales
FROM cleaned_sales_data
GROUP BY TransactionDate
ORDER BY TransactionDate;

-- Return Rate Analysis
SELECT 
    COUNT(CASE WHEN ReturnFlag = 1 THEN 1 END) AS Total_Returns, 
    COUNT(*) AS Total_Orders, 
    ROUND((COUNT(CASE WHEN ReturnFlag = 1 THEN 1 END) * 100.0) / COUNT(*), 2) AS Return_Percentage
FROM cleaned_sales_data;

-- Impact of Discounts on Sales
SELECT 
    CASE WHEN DiscountPercent > 0 THEN 'With Discount' ELSE 'Without Discount' END AS Discount_Category,
    SUM(TransactionAmount) AS Total_Sales
FROM cleaned_sales_data
GROUP BY Discount_Category;

-- Gender-Based Purchase Trends
SELECT CustomerGender, COUNT(TransactionID) AS Total_cleaned_sales_data, SUM(TransactionAmount) AS Total_Sales
FROM cleaned_sales_data
GROUP BY CustomerGender;

-- Age Group-wise Revenue Contribution
SELECT 
    CASE 
        WHEN CustomerAge BETWEEN 18 AND 25 THEN '18-25'
        WHEN CustomerAge BETWEEN 26 AND 35 THEN '26-35'
        WHEN CustomerAge BETWEEN 36 AND 45 THEN '36-45'
        WHEN CustomerAge BETWEEN 46 AND 60 THEN '46-60'
        ELSE '60+'
    END AS Age_Group,
    COUNT(TransactionID) AS Total_cleaned_sales_data, 
    SUM(TransactionAmount) AS Total_Sales
FROM cleaned_sales_data
GROUP BY Age_Group
ORDER BY Age_Group;

-- Shipping Cost Impact on Order Value
SELECT 
    CASE 
        WHEN ShippingCost < 50 THEN 'Low Shipping Cost'
        WHEN ShippingCost BETWEEN 50 AND 100 THEN 'Medium Shipping Cost'
        ELSE 'High Shipping Cost'
    END AS Shipping_Cost_Category,
    ROUND(AVG(TransactionAmount), 2) AS Avg_Order_Value
FROM cleaned_sales_data
GROUP BY Shipping_Cost_Category;

-- Bestselling Products by Revenue
SELECT ProductName, SUM(TransactionAmount) AS TotalRevenue
FROM cleaned_sales_data
GROUP BY ProductName
ORDER BY TotalRevenue DESC;

-- Bestselling Products by Quantity
SELECT ProductName, SUM(Quantity) AS TotalQuantity
FROM cleaned_sales_data
GROUP BY ProductName
ORDER BY TotalQuantity DESC;

-- Average Discount Percentage per Product
SELECT ProductName, AVG(DiscountPercent) AS AvgDiscount
FROM cleaned_sales_data
GROUP BY ProductName
ORDER BY AvgDiscount DESC;

-- Customer Demographics (Age and Gender Distribution)
SELECT CustomerAge, CustomerGender, COUNT(*) AS CustomerCount
FROM cleaned_sales_data
GROUP BY CustomerAge, CustomerGender
ORDER BY CustomerAge, CustomerGender;

-- Loyalty Points Distribution
SELECT LoyaltyPoints, COUNT(*) AS CustomerCount
FROM cleaned_sales_data
GROUP BY LoyaltyPoints
ORDER BY LoyaltyPoints;

-- Purchase Behavior (Frequency and Average Spend per Customer)
SELECT CustomerID, COUNT(*) AS PurchaseFrequency, AVG(TransactionAmount) AS AvgSpend
FROM cleaned_sales_data
GROUP BY CustomerID
ORDER BY PurchaseFrequency DESC, AvgSpend DESC;

-- Sales Distribution across Regions
SELECT Region, SUM(TransactionAmount) AS TotalRevenue
FROM cleaned_sales_data
GROUP BY Region
ORDER BY TotalRevenue DESC;

-- Popular Products in Each Region
SELECT Region, ProductName, SUM(TransactionAmount) AS TotalRevenue
FROM cleaned_sales_data
GROUP BY Region, ProductName
ORDER BY Region, TotalRevenue DESC;

-- In-Store vs. Online Sales
SELECT StoreType, SUM(TransactionAmount) AS TotalRevenue
FROM cleaned_sales_data
GROUP BY StoreType;

-- Sales Performance by City
SELECT City, SUM(TransactionAmount) AS TotalRevenue
FROM cleaned_sales_data
GROUP BY City
ORDER BY TotalRevenue DESC;

-- Popular Payment Methods
SELECT PaymentMethod, SUM(TransactionAmount) AS TotalRevenue
FROM cleaned_sales_data
GROUP BY PaymentMethod
ORDER BY TotalRevenue DESC;

-- Average Shipping Cost
SELECT AVG(ShippingCost) AS AvgShippingCost FROM cleaned_sales_data;

-- Average Delivery Time
SELECT AVG(DeliveryTimeDays) AS AvgDeliveryTime FROM cleaned_sales_data;

-- Impact of delivery time on customer feedback (example)
SELECT DeliveryTimeDays, AVG(FeedbackScore) AS AvgFeedbackScore
FROM cleaned_sales_data
GROUP BY DeliveryTimeDays
ORDER BY DeliveryTimeDays;

-- Distribution of Feedback Scores
SELECT FeedbackScore, COUNT(*) AS Count
FROM cleaned_sales_data
GROUP BY FeedbackScore
ORDER BY FeedbackScore;

SELECT CustomerID, 
       SUM(TransactionAmount) AS Total_Spent, 
       COUNT(TransactionID) AS Purchase_Count,
       ROUND(AVG(TransactionAmount), 2) AS Avg_Order_Value,
       MAX(StoreType) AS Preferred_StoreType,
       MAX(PaymentMethod) AS Preferred_PaymentMethod
FROM cleaned_sales_data
GROUP BY CustomerID
ORDER BY Total_Spent DESC
LIMIT (SELECT COUNT(DISTINCT CustomerID) * 0.05 FROM cleaned_sales_data);

SELECT CustomerID, 
       MAX(TransactionDate) AS Last_Purchase_Date, 
       SUM(TransactionAmount) AS Total_Spent, 
       COUNT(TransactionID) AS Total_Orders, 
       MAX(DiscountPercent) AS Last_Discount_Used
FROM cleaned_sales_data
GROUP BY CustomerID
HAVING Last_Purchase_Date < DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH);

SELECT 
    CASE 
        WHEN LoyaltyPoints >= (SELECT AVG(LoyaltyPoints) FROM cleaned_sales_data) 
        THEN 'High Loyalty' ELSE 'Low Loyalty' 
    END AS Loyalty_Category,
    COUNT(TransactionID) AS Total_Transactions,
    ROUND(AVG(TransactionAmount), 2) AS Avg_Order_Value
FROM cleaned_sales_data
GROUP BY Loyalty_Category;

SELECT CustomerID, 
       COUNT(TransactionID) AS Total_Orders,
       SUM(TransactionAmount) AS Total_Revenue, 
       ROUND(AVG(TransactionAmount), 2) AS Avg_Order_Value, 
       MAX(TransactionDate) - MIN(TransactionDate) AS Customer_Lifespan_Days
FROM cleaned_sales_data
GROUP BY CustomerID;

SELECT 
    DiscountPercent, 
    SUM(TransactionAmount) AS Total_Revenue, 
    COUNT(TransactionID) AS Total_Orders,
    ROUND(AVG(TransactionAmount), 2) AS Avg_Order_Value
FROM cleaned_sales_data
GROUP BY DiscountPercent
ORDER BY DiscountPercent;

SELECT A.ProductName AS Product_A, 
       B.ProductName AS Product_B, 
       COUNT(*) AS Frequency
FROM cleaned_sales_data A
JOIN cleaned_sales_data B ON A.TransactionID = B.TransactionID 
                         AND A.ProductName <> B.ProductName
GROUP BY Product_A, Product_B
ORDER BY Frequency DESC
LIMIT 10;

SELECT ProductName, 
       SUM(Quantity) AS Total_Sold, 
       COUNT(TransactionID) AS Total_Transactions
FROM cleaned_sales_data
GROUP BY ProductName
HAVING Total_Sold < (SELECT AVG(Quantity) FROM cleaned_sales_data)
ORDER BY Total_Sold ASC
LIMIT 10;

SELECT 
    CASE 
        WHEN ShippingCost < 50 THEN 'Low Shipping Cost'
        WHEN ShippingCost BETWEEN 50 AND 100 THEN 'Medium Shipping Cost'
        ELSE 'High Shipping Cost'
    END AS Shipping_Cost_Category,
    COUNT(TransactionID) AS Total_Orders,
    ROUND(AVG(TransactionAmount), 2) AS Avg_Order_Value
FROM cleaned_sales_data
GROUP BY Shipping_Cost_Category;

SELECT Region, 
       AVG(DeliveryTimeDays) AS Avg_Delivery_Days, 
       ROUND(AVG(FeedbackScore), 2) AS Avg_Feedback_Score
FROM cleaned_sales_data
GROUP BY Region
ORDER BY Avg_Delivery_Days DESC
LIMIT 10;

SELECT CustomerID, 
       COUNT(TransactionID) AS Total_Transactions,
       SUM(TransactionAmount) AS Total_Spent, 
       COUNT(CASE WHEN Returned = 'Yes' THEN 1 END) AS Total_Returns,
       COUNT(CASE WHEN DiscountPercent > 50 THEN 1 END) AS High_Discount_Orders
FROM cleaned_sales_data
GROUP BY CustomerID
HAVING Total_Returns > 5 OR High_Discount_Orders > 3
ORDER BY Total_Spent DESC;

SELECT 
    CASE WHEN DiscountPercent > 0 THEN 'Discounted' ELSE 'Non-Discounted' END AS Sale_Type,
    SUM(TransactionAmount) AS Total_Sales,
    COUNT(TransactionID) AS Total_Transactions
FROM cleaned_sales_data
GROUP BY Sale_Type;

SELECT
    CORR(ShippingCost, FeedbackScore) AS ShippingCostFeedbackCorrelation,
    CORR(DeliveryTimeDays, FeedbackScore) AS DeliveryTimeFeedbackCorrelation
FROM cleaned_sales_data;

SELECT
    CASE
        WHEN DiscountPercent BETWEEN 0 AND 10 THEN '0-10%'
        WHEN DiscountPercent BETWEEN 10 AND 20 THEN '10-20%'
        WHEN DiscountPercent BETWEEN 20 AND 30 THEN '20-30%'
        WHEN DiscountPercent BETWEEN 30 AND 40 THEN '30-40%'
        ELSE '40%+'
    END AS DiscountRange,
    AVG(TransactionAmount) AS AvgTransactionAmount,
    COUNT(*) AS TransactionCount,
    SUM(TransactionAmount) AS TotalRevenue
FROM cleaned_sales_data
GROUP BY DiscountRange
ORDER BY DiscountRange;

SELECT
    CustomerID,
    SUM(TransactionAmount) AS TotalSpend,
    COUNT(*) AS PurchaseFrequency,
    CASE
        WHEN SUM(TransactionAmount) > (SELECT AVG(TransactionAmount) FROM cleaned_sales_data) AND COUNT(*) > (SELECT AVG(PurchaseCount) FROM (SELECT CustomerID, COUNT(*) AS PurchaseCount FROM cleaned_sales_data GROUP BY CustomerID)) THEN 'High Value'
        WHEN SUM(TransactionAmount) > (SELECT AVG(TransactionAmount) FROM cleaned_sales_data) THEN 'High Spender'
        WHEN COUNT(*) > (SELECT AVG(PurchaseCount) FROM (SELECT CustomerID, COUNT(*) AS PurchaseCount FROM cleaned_sales_data GROUP BY CustomerID)) THEN 'Frequent Buyer'
        ELSE 'Low Value'
    END AS CustomerSegment
FROM cleaned_sales_data
WHERE CustomerID IS NOT NULL
GROUP BY CustomerID
ORDER BY TotalSpend DESC;

SELECT
    CustomerID,
    SUM(TransactionAmount) AS TotalSpend,
    COUNT(*) AS PurchaseFrequency,
    AVG(LoyaltyPoints) AS AvgLoyaltyPoints,
    (SUM(TransactionAmount) * COUNT(*) * (1 + AVG(LoyaltyPoints)/1000)) AS DailyCustomerValue -- Example formula
FROM cleaned_sales_data
WHERE CustomerID IS NOT NULL
GROUP BY CustomerID
ORDER BY DailyCustomerValue DESC;

SELECT
    CASE
        WHEN DiscountPercent < 10 THEN '0-10%'
        WHEN DiscountPercent < 20 THEN '10-20%'
        WHEN DiscountPercent < 30 THEN '20-30%'
        WHEN DiscountPercent < 40 THEN '30-40%'
        ELSE '40%+'
    END AS DiscountTier,
    AVG(TransactionAmount * (1 - DiscountPercent/100)) AS AvgRevenueAfterDiscount,
    COUNT(*) AS TransactionCount
FROM cleaned_sales_data
GROUP BY DiscountTier
ORDER BY AvgRevenueAfterDiscount DESC;

SELECT
    Region,
    StoreType,
    ProductName,
    SUM(TransactionAmount) AS TotalRevenue
FROM cleaned_sales_data
GROUP BY Region, StoreType, ProductName
ORDER BY Region, StoreType, TotalRevenue DESC;

SELECT
    CORR(ShippingCost, FeedbackScore) AS ShippingCostFeedbackCorrelation
FROM cleaned_sales_data
WHERE ShippingCost IS NOT NULL AND FeedbackScore IS NOT NULL;

