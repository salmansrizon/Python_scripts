With cartup_master AS (
--Cartup Masterfile

WITH ranked_transactions AS (
WITH transactions AS (WITH FilteredData AS (
    SELECT *, m."UpdatedAt" AS "RankingDate"
    FROM public."vw_Transactions_cartup" m
    WHERE m."TransactionStatus" IS NOT NULL AND m."TransactionStatus" <> ''
)
SELECT 
    *, 
      ROW_NUMBER() OVER (
        PARTITION BY "OrderRefId" 
        ORDER BY "TransactionStatus", "RankingDate" DESC  ) AS "Rank"
FROM FilteredData)
SELECT * FROM transactions
WHERE "Rank" = '1')

, order_master AS (
	
	SELECT
		CAST(a."CreatedAt" AS DATE) AS "OrderDate",
		"OrderReferenceId" AS "CustomerOrderCode",
		a."Id" AS "OrderId",
		"OrderCode" AS "SellerOrderCode",
		 "IsCRMConfirm",
		a."CustomerId",
		 CONCAT(b."FirstName",' ',b."LastName") AS "CustomerName",
		a."SellerId",
		"SellerCode",
		e."Name" AS "CustomerStatus",
		g."ProductId",
		g."VariantId",
		g."CategoryId",
		 g."Price" AS "SellingPrice",
		"Quantity",
		g."Price"*"Quantity" AS "TotalPrice",
		h."Id" AS "FreeShippinId",
		COALESCE(CASE WHEN "FreeShippingId" =0 THEN ROUND("ActualShippingCost"*"WeightInKg"*"Quantity"/nullif("TotalWeightInKg",0),2) END,0) AS "CustomerShipping",
		CASE WHEN a."FreeShippingId" IS NOT NULL AND h."IsAdmin" = true THEN ROUND("ActualShippingCost"*"WeightInKg"*"Quantity"/NULLIF("TotalWeightInKg",0),2) ELSE 0 END AS "CartupShipping",
        CASE WHEN a."FreeShippingId" IS NOT NULL AND h."IsAdmin" = false THEN ROUND("ActualShippingCost"*"WeightInKg"*"Quantity"/NULLIF("TotalWeightInKg",0),2) ELSE 0 END AS "SellerShipping",
        a."VoucherCode",
		ROUND("TotalDiscount"*g."Price"*"Quantity"/nullif("TotalItemPrice",0),0) AS "VoucherDiscount",
		COALESCE(CASE WHEN i."SellerId" IS NOT NULL THEN ROUND("TotalDiscount"*g."Price"*"Quantity"/nullif("TotalItemPrice",0),0) END,0) AS "SellerDiscount",
		COALESCE(CASE WHEN i."SellerId" IS NULL THEN ROUND("TotalDiscount"*g."Price"*"Quantity"/nullif("TotalItemPrice",0),0) END,0) AS "CartupDiscount",
		g."Price"*"Quantity"+COALESCE(CASE WHEN "FreeShippingId" = 0 THEN ROUND("ActualShippingCost"*"WeightInKg"*"Quantity"/nullif("TotalWeightInKg",0),2) END,0)-ROUND("TotalDiscount"*g."Price"*"Quantity"/nullif("TotalItemPrice",0),0) AS "OrderValue",
		l."GateWayCode" AS "PaymentMethod",
		"IsPaid",
		CASE WHEN g."IsConsignment" IS TRUE THEN 'Consignment'
			 WHEN "ShopName" IN ('Cartup Retail') THEN 'Retail' ELSE 'MarketPlace' END AS "FulfillmentBy",
		q."Name" AS "CancelReason"
	FROM public."vw_Orders" a
		 LEFT JOIN public."vw_Customers" b ON a."CustomerId" = b."Id"
		 LEFT JOIN public."vw_Seller" c ON a."SellerId" = c."Id"
		 LEFT JOIN public."vw_PackageStatusInfos" d ON a."PackageStatusId"=d."Id"
		LEFT JOIN public."vw_CustomerDeliveryStatus" e ON d."CustomerDeliveryStatusId" = e."Id"
		LEFT JOIN public."vw_OrderItems" g ON a."Id" = g."OrderId"
        LEFT JOIN public."vw_FreeShippingNews" h ON a."FreeShippingId" = h."Id"
		LEFT JOIN public."vw_Vouchers" i ON a."VoucherCode" = i."VoucherCode"
		LEFT JOIN public."vw_PaymentMethods" l ON a."PaymentMode" = l."Id"
		 LEFT JOIN ranked_transactions m ON a."OrderReferenceId" = m."OrderRefId"
		LEFT JOIN public."vw_Reasons" q ON a."CancelReasonId" = q."Id"
		where "GateWayCode" is not null
	ORDER BY "OrderDate","OrderReferenceId"
	),
	category_tree AS (
	
	WITH "L1" AS (
	    SELECT 
	        "Id",
	        "IndustryId",
	        "NameEn"
	    FROM public."vw_Category"
	    WHERE "ParentId" IS NULL
	),
	"L2" AS (
	    SELECT 
	        L1."NameEn" AS cat1,
	        COALESCE(L2."Id", L1."Id") AS "Id",
	        COALESCE(L2."IndustryId", L1."IndustryId") AS "IndustryId",
	        L2."NameEn" AS cat2
	    FROM public."vw_Category" L2
	    RIGHT JOIN "L1" L1 ON L1."Id" = L2."ParentId"
	),
	"L3" AS (
	    SELECT 
	        cat1,
	        cat2,
	        COALESCE(L3."Id", L2."Id") AS "Id",
	        COALESCE(L3."IndustryId", L2."IndustryId") AS "IndustryId",
	        L3."NameEn" AS cat3
	    FROM public."vw_Category" L3
	    RIGHT JOIN "L2" L2 ON L2."Id" = L3."ParentId"
	),
	"L4" AS (
	    SELECT 
	        cat1,
	        cat2,
	        cat3,
	        COALESCE(L4."Id", L3."Id") AS "Id",
	        COALESCE(L4."IndustryId", L3."IndustryId") AS "IndustryId",
	        L4."NameEn" AS cat4
	    FROM public."vw_Category" L4
	    RIGHT JOIN "L3" L3 ON L3."Id" = L4."ParentId"
	),
	"L5" AS (
	    SELECT 
	        cat1,
	        cat2,
	        cat3,
	        cat4,
	        COALESCE(L5."Id", L4."Id") AS "Id",
	        COALESCE(L5."IndustryId", L4."IndustryId") AS "IndustryId",
	        L5."NameEn" AS cat5
	    FROM public."vw_Category" L5
	    RIGHT JOIN "L4" L4 ON L4."Id" = L5."ParentId"
	),
	"L6" AS (
	    SELECT 
	        COALESCE(L6."IndustryId", L5."IndustryId") AS vertical,
	        cat1,
	        cat2,
	        cat3,
	        cat4,
	        cat5,
	        L6."NameEn" AS cat6,
	        COALESCE(L6."Id", L5."Id") AS "Id"
	    FROM public."vw_Category" L6
	    RIGHT JOIN "L5" L5 ON L5."Id" = L6."ParentId"
	)
	SELECT
	     v."Name" AS "Vertical",
	    cat3,
	    l."Id" AS category_id
	FROM "L6" l
	LEFT JOIN public."vw_Industries" v ON l."vertical" = v."Id"
	ORDER BY "Vertical"
	),
	return_master AS (
	
	WITH ranked AS (
	SELECT
     "OrderReferenceId",
    f."Name" AS "ReturnReason",
    "IsFailedDelivery",
     "VariantId",
    "IsQC",
	CASE WHEN "IsFailedDelivery" = FALSE THEN ROW_NUMBER() OVER (
		    PARTITION BY a."OrderReferenceId","VariantId"
		    ORDER BY c."CreatedAt" DESC) ELSE '1' END
				 AS "Rank"

FROM public."vw_OrderReturns" a
LEFT JOIN public."vw_OrderReturnItems" b ON a."Id" = b."OrderReturnId"
LEFT JOIN public."vw_OrderReturnPackages" c ON a."Id" = c."OrderReturnId"
LEFT JOIN public."vw_Reasons" f ON b."ReturnReasonId" = f."Id"
WHERE c."CreatedAt" IS NOT NULL
GROUP BY
	 c."CreatedAt",
    "OrderReferenceId",
    f."Name",
    "IsFailedDelivery",
    "VariantId",
     "IsQC"
	)
	SELECT * FROM ranked
	WHERE "Rank" = '1'
	),
return_qty AS (
WITH returned AS (SELECT
    "OrderReferenceId",
     "IsFailedDelivery",
    "VariantId",
    SUM("Quantity") AS "Quantity",
     "IsQC",
	CASE WHEN "IsFailedDelivery" = FALSE THEN ROW_NUMBER() OVER (
		    PARTITION BY a."OrderReferenceId","VariantId"
		    ORDER BY c."CreatedAt" DESC) ELSE '1' END
				 AS "Rank"

FROM public."vw_OrderReturns" a
LEFT JOIN public."vw_OrderReturnItems" b ON a."Id" = b."OrderReturnId"
LEFT JOIN public."vw_OrderReturnPackages" c ON a."Id" = c."OrderReturnId"
WHERE c."CreatedAt" IS NOT NULL
GROUP BY
	c."CreatedAt",
    "OrderReferenceId",
     "IsFailedDelivery",
    "VariantId",
     "IsQC"
ORDER BY a."OrderReferenceId","VariantId")

SELECT
	"OrderReferenceId",
	"VariantId",
	SUM("Quantity") AS total_returns
FROM returned
WHERE "IsFailedDelivery" IS FALSE
AND "IsQC" IS TRUE
GROUP BY "OrderReferenceId",
	"VariantId"),
	
	
fwd_pkg AS (
SELECT
	"OrderId",
	"PackageCode",
	"DeliveryStatusId"
FROM "vw_OrderPackages"
WHERE "IsQC" IS FALSE
)
	
	SELECT
	om."CustomerOrderCode",
		"SellerOrderCode",
		 "IsCRMConfirm",
		om."CustomerId",
		"CustomerName",
		om."SellerId",
		"SellerCode",
		COALESCE(e."Name","CustomerStatus") AS "CustomerStatus",
		om."ProductId",
		om."VariantId",
		"CategoryId",
		"cat3",
		om."Quantity",
		"TotalPrice",
		"FreeShippinId",
		"CustomerShipping",
		"SellerShipping",
		"CartupShipping",
		"VoucherCode",
		"SellerDiscount",
		"CartupDiscount",
		"OrderValue",
		"PaymentMethod",
		 "IsPaid",
	    "FulfillmentBy",
		"CancelReason",
		"ReturnReason",
		"IsFailedDelivery",
		COALESCE(rq."total_returns",om."Quantity") AS "ReturnQty",
		"IsQC",
		COALESCE("SellingPrice"*rq."total_returns"+"CustomerShipping"*rq."total_returns"/nullif(om."Quantity",0)-"VoucherDiscount"*rq."total_returns"/nullif(om."Quantity",0),"OrderValue") AS "RefundAmount",
		"OrderDate"
	FROM order_master om
	LEFT JOIN category_tree ct ON om."CategoryId" = ct."category_id"
	LEFT JOIN return_master rm ON om."CustomerOrderCode" = rm."OrderReferenceId" AND om."VariantId" = rm."VariantId"
	LEFT JOIN fwd_pkg d ON om."OrderId" = d."OrderId"
	LEFT JOIN return_qty rq ON om."CustomerOrderCode" = rq."OrderReferenceId" AND om."VariantId" = rq."VariantId"
	LEFT JOIN public."vw_PackageStatusInfos" e ON d."DeliveryStatusId" = e."Id"

	WHERE 
        "OrderDate" >= '2024-10-28'
		AND ("PaymentMethod" IS NOT NULL OR "IsPaid" IS TRUE)
		
ORDER BY 
	"OrderDate", "CustomerOrderCode"

	),
base AS (
    SELECT 
        DISTINCT "CustomerId",
        "OrderDate" AS "Date"
    FROM cartup_master cm
    WHERE 
        "OrderDate" >= '2024-10-28'
		AND ("PaymentMethod" IS NOT NULL OR "IsPaid" IS TRUE)
		
),
classify AS (
SELECT  
    "Date",
    "CustomerId",
    ROW_NUMBER() OVER (PARTITION BY "CustomerId" ORDER BY "Date" ASC) AS "Rank"
FROM base
ORDER BY "Date"),

user_type AS (
Select
	"Date",
	SUM(CASE WHEN "Rank" = 1 THEN 1 ELSE 0 END) AS "NewUser",
    SUM(CASE WHEN "Rank" > 1 THEN 1 ELSE 0 END) AS "ReturningUser"
FROM classify

GROUP BY "Date"
ORDER BY "Date"
),
	date_diff AS (
	SELECT "Date",
		"CustomerId",
		CASE WHEN "Rank" = 1 THEN 1 END AS "New",
		CASE WHEN "Rank" > 1 THEN 1 END AS "Returning",
		COALESCE("Date" - LAG("Date") OVER (PARTITION BY "CustomerId" ORDER BY "Date"), 0) AS "date_diff"
	FROM classify
	GROUP BY "Date", "CustomerId", "Rank"
	ORDER BY "Date", "CustomerId"),

cohort AS (SELECT
    "Date",
	ROUND(COUNT(DISTINCT CASE WHEN "date_diff" BETWEEN 0 AND 7 THEN "CustomerId" END) * 100.0 / count(DiSTINCT "CustomerId"),2) || '%' AS "0-7_days",
	ROUND(COUNT(DISTINCT CASE WHEN "date_diff" BETWEEN 8 AND 15 THEN "CustomerId" END) * 100.0 / count(DiSTINCT "CustomerId"),2) || '%' AS "8-15_days",
	ROUND(COUNT(DISTINCT CASE WHEN "date_diff" BETWEEN 16 AND 30 THEN "CustomerId" END) * 100.0 / count(DiSTINCT "CustomerId"),2) || '%' AS "16-30_days",
	ROUND(COUNT(DISTINCT CASE WHEN "date_diff" BETWEEN 31 AND 45 THEN "CustomerId" END) * 100.0 / count(DiSTINCT "CustomerId"),2) || '%' AS "31-45_days",
	ROUND(COUNT(DISTINCT CASE WHEN "date_diff" BETWEEN 46 AND 60 THEN "CustomerId" END) * 100.0 / count(DiSTINCT "CustomerId"),2) || '%' AS "46-60_days",
	ROUND(COUNT(DISTINCT CASE WHEN "date_diff" > 60 THEN "CustomerId" END) * 100.0 / count(DiSTINCT "CustomerId"),2) || '%' AS "More_than_60_days"
FROM date_diff
WHERE "date_diff" > 0 
GROUP BY "Date"
ORDER BY "Date"
	)


SELECT DISTINCT
    cm."OrderDate",
    TO_CHAR(cm."OrderDate", 'Day') AS "Weekday", 
	EXTRACT('Month' FROM (cm."OrderDate")) as "Month",
    COUNT(DISTINCT "CustomerOrderCode") AS "GrossOrders",
	COUNT(DISTINCT CASE WHEN "CustomerStatus" = 'Delivered' THEN "CustomerOrderCode" END ) AS "TotalDeliveredOrders",
	COUNT(DISTINCT CASE WHEN ("SellerShipping" + "CartupShipping" = 0 AND "SellerDiscount" + "CartupDiscount" = 0 ) THEN "CustomerOrderCode" END) AS "TotalNonAssistedOrders",
	COUNT(DISTINCT "CustomerOrderCode") - COUNT(DISTINCT CASE WHEN ("SellerShipping" + "CartupShipping" = 0 AND "SellerDiscount" + "CartupDiscount" = 0 ) THEN "CustomerOrderCode" END) AS "AssistedOrders",
    '0' AS "B2B Orders",
    COALESCE(COUNT(DISTINCT CASE WHEN ("SellerId" ='2' ) THEN "SellerOrderCode" END), 0) AS "Retail Orders",
	ROUND(
    CASE 
        WHEN COUNT(DISTINCT "CustomerOrderCode") = 0 THEN 0 
        ELSE 
            CAST(COUNT(DISTINCT CASE WHEN "CustomerStatus" = 'Delivered' THEN "CustomerOrderCode" END) AS NUMERIC) 
            / COUNT(DISTINCT "CustomerOrderCode") 
    END, 
    2
) AS "G2N",
    SUM("Quantity") AS "TotalItems",
	COUNT(DISTINCT "CustomerId") AS "TotalBuyers",
	U."NewUser" AS "TotalNewBuyers",
    U."ReturningUser" AS "TotalReturningBuyers",
	COUNT(DISTINCT CASE WHEN ("SellerShipping" + "CartupShipping" = 0 AND "SellerDiscount" + "CartupDiscount" = 0 ) THEN "CustomerId" END) AS "NonAssistedBuyer",
    COUNT(DISTINCT "CustomerId") - COUNT(DISTINCT CASE WHEN ("SellerShipping" + "CartupShipping" = 0 AND "SellerDiscount" + "CartupDiscount" = 0  ) THEN "CustomerId" END) AS "AssistedBuyer",
	COALESCE(COUNT(DISTINCT CASE WHEN "SellerCode" = 'B2B' THEN "CustomerId" END),0) AS "B2B Buyer",
	COALESCE(COUNT(DISTINCT CASE WHEN "SellerId" = '2' THEN "CustomerId" END),0) AS "Retail Buyer",
	COALESCE(COUNT(DISTINCT CASE WHEN "FulfillmentBy" = 'Retail' THEN "ProductId" END), 0) AS "GrossFbcItems",
    SUM("TotalPrice") AS "TotalGMV",
    COALESCE(SUM(CASE WHEN ("SellerShipping" + "CartupShipping" = 0 AND "SellerDiscount" + "CartupDiscount" = 0  ) THEN "TotalPrice" END), 0) AS "NonAssistedGMV",
    SUM("TotalPrice") - COALESCE(SUM(CASE WHEN ("SellerShipping" + "CartupShipping" = 0 AND "SellerDiscount" + "CartupDiscount" = 0  ) THEN "TotalPrice" END), 0) AS "AssistedGMV",
    SUM("CartupShipping"+"CartupDiscount"+"SellerShipping"+"SellerDiscount") AS "TotalSubsidy",
    COALESCE(SUM(CASE WHEN ("SellerCode" IS NULL) THEN "TotalPrice" END), 0) AS "B2B GMV",
    COALESCE(COUNT(DISTINCT CASE WHEN ("SellerId" = NULL) THEN "CustomerId" END), 0) AS "B2B Buyers",
    COALESCE(SUM(CASE WHEN ("SellerId" = '2') THEN "TotalPrice" END), 0) AS "Retail GMV",
    COALESCE(COUNT(DISTINCT CASE WHEN ("SellerId" = '2') THEN "CustomerId" END), 0) AS "Retail Buyers",
    SUM( COALESCE("SellerDiscount", 0) + COALESCE("CartupDiscount", 0) +  COALESCE("SellerShipping", 0) + COALESCE("CartupShipping", 0)) AS "TotalCustomerSubsidy",
    SUM(COALESCE("CartupDiscount", 0) + COALESCE("CartupShipping", 0)) AS "TotalPlatformSubsidy",
    SUM(COALESCE("SellerDiscount", 0) + COALESCE("SellerShipping", 0)) AS "TotalSellerSubsidy",
    SUM(COALESCE("SellerShipping", 0) + COALESCE("CartupShipping", 0)) AS "TotalShippingSubsidy",
    SUM(COALESCE("CartupShipping", 0)) AS "TotalPlatformShippingSubsidy",
    SUM(COALESCE("SellerShipping", 0)) AS "TotalSellerShippingSubsidy",
    SUM(COALESCE("CustomerShipping", 0)) AS "TotalCustomerShipping",
	"0-7_days",
	"8-15_days",
	"16-30_days",
	"31-45_days",
	"46-60_days",
	"More_than_60_days",
	SUM(COALESCE("CartupDiscount", 0)) AS "TotalCartupDiscount",
    COALESCE(SUM(CASE WHEN ("SellerId" = '2') THEN "CartupDiscount" END),0) AS "Cartup_Retail_Discount",
    COALESCE(SUM(CASE WHEN ("SellerId" = '2') THEN "CartupShipping" END),0) AS "Cartup_Retail_Shipping",
	SUM(CASE WHEN "cat3" NOT IN ('Standard Bikes','Scooters') THEN "TotalPrice" END) AS "TotalGMVwithoutBikes",
	COALESCE(SUM(CASE WHEN ("FreeShippinId" NOT IN ('0') OR "FreeShippinId" IS NOT NULL) AND "cat3" NOT IN ('Standard Bikes','Scooters') THEN "TotalPrice" END),0) AS "FreeshippinGMV",
	COALESCE(SUM(CASE WHEN "VoucherCode" IS NOT NULL AND "cat3" NOT IN ('Standard Bikes','Scooters') THEN "TotalPrice" END),0) AS "VoucherGMV",
	COALESCE(SUM(CASE WHEN "SellerId" IN ('2') THEN "Quantity" END),0) AS "RetailItems",
	SUM(CASE WHEN "CustomerStatus" = 'Delivered' THEN "TotalPrice" END) AS "NMV",

	SUM(CASE WHEN "IsFailedDelivery" = FALSE AND "IsQC" = TRUE THEN "RefundAmount" END) AS "RMV",
	COUNT(DISTINCT CASE WHEN ("IsFailedDelivery" IS NULL OR "IsFailedDelivery" IS FALSE) AND "CustomerStatus" = 'Delivered' THEN "CustomerOrderCode" END) AS "NOS",
	SUM(CASE WHEN "CustomerStatus" = 'Delivered' THEN "Quantity" END) AS "NIS",
	COUNT(DISTINCT CASE WHEN "IsFailedDelivery" = TRUE THEN "CustomerOrderCode" END) AS "FailedDelivery",
	COUNT(DISTINCT CASE WHEN "IsFailedDelivery" IS NULL AND  "CancelReason" IS NOT NULL THEN  "SellerOrderCode"END) AS "TotalCancelledOrders",
	COUNT(DISTINCT CASE WHEN "IsFailedDelivery" IS NULL   AND  "CancelReason" IN ('Out of stock','Sourcing delay','Wrong price') THEN  "SellerOrderCode" END) AS "SellerCancelledOrders",
    COALESCE(COUNT(DISTINCT CASE 
			    WHEN "IsQC" =true 
					 AND COALESCE("IsFailedDelivery", 'False') = 'False' AND ("ReturnReason" ILIKE '%Item is Damaged%'
				OR "ReturnReason" ILIKE '%I received an empty box%'
				OR "ReturnReason" ILIKE '%The item has a defect or is not test%'
				OR "ReturnReason" ILIKE '%Received expired item(s)%'
				OR "ReturnReason" ILIKE '%Some accessories or parts of the product are missing%'
				OR "ReturnReason" ILIKE '%The item has a defect or is not working properly%'
				OR "ReturnReason" ILIKE '%The item does not match the description or photo%'
				OR "ReturnReason" ILIKE '%I received wrong item%'
				OR "ReturnReason" ILIKE '%Item(s) are missing%'
				OR "ReturnReason" ILIKE '%Item(s) expired%'
				OR "ReturnReason" ILIKE '%The item is damaged or broken%'
				OR "ReturnReason" ILIKE '%Wrong Item%'
				OR "ReturnReason" ILIKE '%Item is fake/Counterfeit%'
				OR "ReturnReason" ILIKE '%Item is Defected%'
				OR "ReturnReason" ILIKE '%Received an empty/suspicious parcel%'
				OR "ReturnReason" ILIKE '%Not as advertised%'
				OR "ReturnReason" ILIKE '%Defective%'
				OR "ReturnReason" ILIKE '%Damaged%'
				OR "ReturnReason" ILIKE '%Expired Items%'
				OR "ReturnReason" ILIKE '%I changed my mind and no longer want the item%'
				OR "ReturnReason" ILIKE '%Missing accessories/partial products received%'
				OR "ReturnReason" ILIKE '%Change of mind%'
				OR "ReturnReason" ILIKE '%Received an empty Box%'
				OR "ReturnReason" ILIKE '%Product Expired%'
				OR "ReturnReason" ILIKE '%Missing Item(s)%')
							THEN "ReturnQty"
					   END),0) 
					AS "Total_Quality_returns",
     COALESCE(SUM(
     CASE 
        WHEN "FulfillmentBy" = 'Consignment' AND "TotalPrice" IS NOT NULL 
        THEN "TotalPrice" 
        ELSE 0 
    END
    ), 0) AS "Consignment GMV",
    COALESCE(COUNT(DISTINCT CASE WHEN "FulfillmentBy" = 'Consignment'  THEN "CustomerOrderCode" END), 0) AS "Consignment Orders",
	COALESCE(COUNT(DISTINCT CASE WHEN "FulfillmentBy" = 'Consignment' THEN "CustomerId" END),0) AS "Consignment Buyer",
	COALESCE(SUM(CASE WHEN "FulfillmentBy" = 'Consignment' THEN "Quantity" ELSE 0 END), 0) AS "Consignment Items",
	SUM(CASE WHEN "CustomerStatus" = 'Delivered' AND "SellerId" IN ('2') THEN "TotalPrice" END) AS "Retail NMV",
	COUNT(DISTINCT CASE WHEN ("IsFailedDelivery" IS NULL OR "IsFailedDelivery" IS FALSE) AND "CustomerStatus" = 'Delivered' AND "SellerId" IN ('2') THEN "CustomerOrderCode" END) AS "Retail NOS",
	' ' as " ",
	' ' as " ",
	"OrderDate",
	COUNT(DISTINCT "CustomerOrderCode") AS "CreatedOrders",
	COUNT(DISTINCT "CustomerId") AS "Buyers",
    COUNT(DISTINCT CASE WHEN "IsCRMConfirm" = 'true' THEN "CustomerOrderCode" ELSE NULL END) AS "Net Orders",
	COUNT(DISTINCT CASE WHEN "IsCRMConfirm" = 'true' THEN "CustomerId" ELSE NULL END) AS "Net Buyers"
					
FROM "cartup_master" cm
left join "user_type" U on cm."OrderDate" = U."Date"
left join "cohort" C on cm."OrderDate" = C."Date"
WHERE "OrderDate" < CURRENT_DATE
GROUP BY cm."OrderDate",
	TO_CHAR(cm."OrderDate", 'Day'),
    U."NewUser" ,
    U."ReturningUser",
	"0-7_days",
	"8-15_days",
	"16-30_days",
	"31-45_days",
	"46-60_days",
	"More_than_60_days";