--Cartup Masterfile

WITH ranked_transactions AS (
WITH transactions AS (WITH FilteredData AS (
    SELECT *, m."UpdatedAt" AS "RankingDate"
    FROM "vw_Transactions" m
	LEFT JOIN public."vw_TransactionsMTB" r ON m."Id" = r."TransactionId"
	LEFT JOIN public."vw_TransactionsSEB" s ON m."Id" = s."TransactionId"
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
		"IsSellerConfirm",
		a."CustomerId",
		CONCAT(b."FirstName",' ',b."LastName") AS "CustomerName",
		n."State",
		n."City",
		n."Zone",
		o."Name" AS "DestinationHub",
		a."SellerId",
		"SellerCode",
		"ShopName",
		c."ApprovalStatusId" AS "SellerApprovalStatus",
		p."Name" AS "SourceHub",
		d."Name" AS "LogisticsStatus",
		e."Name" AS "CustomerStatus",
		f."Name" AS "SellerStatus",
		g."ProductId",
		g."VariantId",
		g."ShopSKU",	
		j."NameEn" AS "ProductName",
		k."Name" AS "VariantName",
		g."CategoryId",
		"WeightInKg",
		k."CurrentStockQty",
		k."Price" AS "MRP",
		j."ApprovalStatusId" AS "SKUStatus",
		g."Price" AS "SellingPrice",
		"Quantity",
		g."Price"*"Quantity" AS "TotalPrice",
		ROUND("ActualShippingCost"*"WeightInKg"*"Quantity"/nullif("TotalWeightInKg",0),2) as "ShippingCost",
		h."Id" AS "FreeShippinId",
		COALESCE(CASE WHEN "FreeShippingId" = 0 THEN ROUND("ActualShippingCost"*"WeightInKg"*"Quantity"/nullif("TotalWeightInKg",0),2) END,0) AS "CustomerShipping",
		ROUND(CASE WHEN h."IsFromAdmin" = 'true' THEN ROUND("ActualShippingCost"*"WeightInKg"*"Quantity"/nullif("TotalWeightInKg",0),2)*"SellerPercent"/100
					WHEN h."IsFromAdmin" = 'false' THEN ROUND("ActualShippingCost"*"WeightInKg"*"Quantity"/nullif("TotalWeightInKg",0),2) 
					ELSE 0 END,2) AS "SellerShipping",
	    ROUND(CASE WHEN h."IsFromAdmin" = 'true' THEN ROUND("ActualShippingCost"*"WeightInKg"*"Quantity"/nullif("TotalWeightInKg",0),2)*"AdminPercent"/100 ELSE 0 END,2) AS "CartupShipping",
		a."VoucherCode",
		ROUND("TotalDiscount"*g."Price"*"Quantity"/nullif("TotalItemPrice",0),0) AS "VoucherDiscount",
		COALESCE(CASE WHEN i."SellerId" IS NOT NULL THEN ROUND("TotalDiscount"*g."Price"*"Quantity"/nullif("TotalItemPrice",0),0) END,0) AS "SellerDiscount",
		COALESCE(CASE WHEN i."SellerId" IS NULL THEN ROUND("TotalDiscount"*g."Price"*"Quantity"/nullif("TotalItemPrice",0),0) END,0) AS "CartupDiscount",
		g."Price"*"Quantity"+COALESCE(CASE WHEN "FreeShippingId" = 0 THEN ROUND("ActualShippingCost"*"WeightInKg"*"Quantity"/nullif("TotalWeightInKg",0),2) END,0)-ROUND("TotalDiscount"*g."Price"*"Quantity"/nullif("TotalItemPrice",0),0) AS "OrderValue",
		l."GateWayCode" AS "PaymentMethod",
		"Amount" AS "TransactionAmount",
		"IsPaid",
		"TransactionStatus",
		NULLIF(array_to_string(
	           ARRAY_REMOVE(ARRAY[
	               CASE WHEN "SslBankTranId" IS NOT NULL THEN "SslBankTranId" END,
				   CASE WHEN "BkashTrxId" IS NOT NULL THEN "BkashTrxId" END,
	               CASE WHEN "NagadPayerReference" IS NOT NULL THEN "NagadPayerReference" END,
	               CASE WHEN "UpayTrxId" IS NOT NULL THEN "UpayTrxId" END,
				   CASE WHEN "EblTrxUUID" IS NOT NULL THEN "EblTrxUUID" END,
				   CASE WHEN "SebTrxUUID" IS NOT NULL THEN "SebTrxUUID" END,
				   CASE WHEN "MtbTransactionId" IS NOT NULL THEN "MtbTransactionId" END
	           ], NULL), ', '), '') AS "TrxID",
		NULLIF(array_to_string(
	           ARRAY_REMOVE(ARRAY[
	               CASE WHEN "SslCardIssuer" IS NOT NULL THEN "SslCardIssuer" END,
				   CASE WHEN "BkashCustomerMsisdn" IS NOT NULL THEN "BkashCustomerMsisdn" END,
	               CASE WHEN "NagadCustomerMsisdn" IS NOT NULL THEN "NagadCustomerMsisdn" END,
	               CASE WHEN "UpayCustomerWallet" IS NOT NULL THEN "UpayCustomerWallet" END,
				   CASE WHEN "EblCardNo" IS NOT NULL THEN "EblCardNo" END,
				   CASE WHEN "MtbOrderCardNo" IS NOT NULL THEN "MtbOrderCardNo" END,
				   CASE WHEN "SebCardNo" IS NOT NULL THEN "SebCardNo" END
	           ], NULL), ', '), '') AS "CardNo",
		CASE WHEN "FulfillmentBy" = 1 THEN 'MarketPlace' ELSE 'Retail' END AS "FulfillmentBy",
		q."Name" AS "CancelReason",
		"CancelReasonDescription",
		CAST(a."UpdatedAt" AS DATE) AS "LastUpdated"
	FROM public."vw_Orders" a
		LEFT JOIN public."vw_Customers" b ON a."CustomerId" = b."Id"
		LEFT JOIN public."vw_Seller" c ON a."SellerId" = c."Id"
		LEFT JOIN public."vw_PackageStatusInfos" d ON a."PackageStatusId"=d."Id"
		LEFT JOIN public."vw_CustomerDeliveryStatus" e ON d."CustomerDeliveryStatusId" = e."Id"
		LEFT JOIN public."vw_SellerDeliveryStatus" f ON d."SellerDeliveryStatusId" = f."Id"
		LEFT JOIN public."vw_OrderItems" g ON a."Id" = g."OrderId"
		LEFT JOIN public."vw_FreeShipping" h ON a."FreeShippingId" = h."Id"
		LEFT JOIN public."vw_Vouchers" i ON a."VoucherCode" = i."VoucherCode"
		LEFT JOIN public."vw_Product" j ON g."ProductId" = j."Id"
		LEFT JOIN public."vw_ProductVariant" k ON g."ShopSKU" = k."ShopSKU"
		LEFT JOIN public."vw_PaymentMethods" l ON a."PaymentMode" = l."Id"
		LEFT JOIN ranked_transactions m ON a."OrderReferenceId" = m."OrderRefId"
		LEFT JOIN public."vw_ShippingInfos" n ON a."Id" = n."OrderId"
		LEFT JOIN public."vw_Stores" o ON a."DestinationHubId" = o."Id"
		LEFT JOIN public."vw_Stores" p ON a."SourceHubId" = p."Id"
		LEFT JOIN public."vw_Reasons" q ON a."CancelReasonId" = q."Id"
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
	    cat1,
	    cat2,
	    cat3,
	    cat4,
	    cat5,
	    cat6,
	    l."Id" AS category_id,
	    COALESCE(cat6, cat5, cat4, cat3, cat2, cat1) AS deepestcategory
	FROM "L6" l
	LEFT JOIN public."vw_Industries" v ON l."vertical" = v."Id"
	ORDER BY "Vertical"
	),
	return_master AS (
	
	WITH ranked AS (
	SELECT
    CAST(a."CreatedAt" AS DATE) AS "ReturnCreatedDate",
    CAST(a."UpdatedAt" AS DATE) AS "ReturnUpdatedDate",
    a."Id",
    a."OrderReturnCode",
    a."OrderPackageCode" as "OriginalPackageCode",
    "OrderReturnPackageCode",
    CAST(c."CreatedAt" AS DATE) AS "ReturnPkgCreatedDate",
    g."Name" AS "CR_SourceHub",
    h."Name" AS "CR_DesHub",
    e."Name" AS "ReturnStatus",
    "CustomerId",
    "OrderReferenceId",
    f."Name" AS "ReturnReason",
    "ReturnReasonDescription",
    i."Name" AS "PickUpCancelReason",
    "IsFailedDelivery",
    "ProductId",
    "VariantId",
    SUM("Quantity") AS "Quantity",
    "ItemPrice",
    "IsQC",
    j."Name" AS "Bank",
    k."BranchName" AS "Branch",
    "AccountName",
    "AccountNumber",
    "Amount",
    "IsReturnVoucher",
	CASE WHEN "IsFailedDelivery" = FALSE THEN ROW_NUMBER() OVER (
		    PARTITION BY a."OrderReferenceId","VariantId"
		    ORDER BY c."CreatedAt" DESC) ELSE '1' END
				 AS "Rank"

FROM public."vw_OrderReturns" a
LEFT JOIN public."vw_OrderReturnItems" b ON a."Id" = b."OrderReturnId"
LEFT JOIN public."vw_OrderReturnPackages" c ON a."Id" = c."OrderReturnId"
LEFT JOIN public."vw_RefundBankDetails" d ON a."Id" = d."OrderReturnId"
LEFT JOIN public."vw_PackageStatusInfos" e ON a."ReturnStatusId" = e."Id"
LEFT JOIN public."vw_Reasons" f ON b."ReturnReasonId" = f."Id"
LEFT JOIN public."vw_Stores" g ON c."DestinationHubId" = g."Id"
LEFT JOIN public."vw_Stores" h ON c."SourceHubId" = h."Id"
LEFT JOIN public."vw_Reasons" i ON a."PickupCancelReasonId" = i."Id"
LEFT JOIN public."vw_Banks" j ON d."BankId" = j."Id"
LEFT JOIN public."vw_Branches" k ON d."BranchId" = k."Id" 
WHERE c."CreatedAt" IS NOT NULL
GROUP BY
    CAST(a."CreatedAt" AS DATE),
    CAST(a."UpdatedAt" AS DATE),
    a."Id",
    a."OrderReturnCode",
    a."OrderPackageCode",
    "OrderReturnPackageCode",
    CAST(c."CreatedAt" AS DATE),
	c."CreatedAt",
    g."Name",
    h."Name",
    e."Name",
    "CustomerId",
    "OrderReferenceId",
    f."Name",
    "ReturnReasonDescription",
    i."Name",
    "IsFailedDelivery",
    "ProductId",
    "VariantId",
    "ItemPrice",
    "IsQC",
    j."Name",
    k."BranchName",
    "AccountName",
    "AccountNumber",
    "Amount",
    "IsReturnVoucher")
	
	SELECT * FROM ranked
	WHERE "Rank" = '1'
	), crm AS (
	SELECT
		"CustomerOrderCode",
		"OrderPackageCode",
		MIN(CASE WHEN "LogisticStatus" = 'Canceled' THEN 
        TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS'), 'YYYY-MM-DD') END) AS "CancelledDate",
		MIN(CASE WHEN "LogisticStatus" = 'Delivered' THEN 
        TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS'), 'YYYY-MM-DD') END) AS "DeliveredDate",
		MIN(CASE WHEN "LogisticStatus" = 'Return Confirm' THEN 
        TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS'), 'YYYY-MM-DD') END) AS "ReturnConfirmDate"
	FROM public."vw_OrderPackageTransactions"
	GROUP BY "CustomerOrderCode", "OrderPackageCode"
	HAVING COALESCE("OrderPackageCode",MIN(CASE WHEN "LogisticStatus" = 'Canceled' THEN 
        TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS'), 'YYYY-MM-DD') END),MIN(CASE WHEN "LogisticStatus" = 'Delivered' THEN 
        TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS'), 'YYYY-MM-DD') END),MIN(CASE WHEN "LogisticStatus" = 'Return Confirm' THEN 
        TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS'), 'YYYY-MM-DD') END)) IS NOT NULL
		), return_qty AS (
		WITH returned AS (SELECT
    CAST(a."CreatedAt" AS DATE) AS "ReturnCreatedDate",
    a."Id",
    a."OrderReturnCode",
    a."OrderPackageCode" as "OriginalPackageCode",
    "OrderReturnPackageCode",
    CAST(c."CreatedAt" AS DATE) AS "ReturnPkgCreatedDate",
    g."Name" AS "CR_SourceHub",
    h."Name" AS "CR_DesHub",
    e."Name" AS "ReturnStatus",
    "CustomerId",
    "OrderReferenceId",
    f."Name" AS "ReturnReason",
    "ReturnReasonDescription",
    i."Name" AS "PickUpCancelReason",
    "IsFailedDelivery",
    "ProductId",
    "VariantId",
    SUM("Quantity") AS "Quantity",
    "ItemPrice",
    "IsQC",
    j."Name" AS "Bank",
    k."BranchName" AS "Branch",
    "AccountName",
    "AccountNumber",
    "Amount",
    "IsReturnVoucher",
	CASE WHEN "IsFailedDelivery" = FALSE THEN ROW_NUMBER() OVER (
		    PARTITION BY a."OrderReferenceId","VariantId"
		    ORDER BY c."CreatedAt" DESC) ELSE '1' END
				 AS "Rank"

FROM public."vw_OrderReturns" a
LEFT JOIN public."vw_OrderReturnItems" b ON a."Id" = b."OrderReturnId"
LEFT JOIN public."vw_OrderReturnPackages" c ON a."Id" = c."OrderReturnId"
LEFT JOIN public."vw_RefundBankDetails" d ON a."Id" = d."OrderReturnId"
LEFT JOIN public."vw_PackageStatusInfos" e ON a."ReturnStatusId" = e."Id"
LEFT JOIN public."vw_Reasons" f ON b."ReturnReasonId" = f."Id"
LEFT JOIN public."vw_Stores" g ON c."DestinationHubId" = g."Id"
LEFT JOIN public."vw_Stores" h ON c."SourceHubId" = h."Id"
LEFT JOIN public."vw_Reasons" i ON a."PickupCancelReasonId" = i."Id"
LEFT JOIN public."vw_Banks" j ON d."BankId" = j."Id"
LEFT JOIN public."vw_Branches" k ON d."BranchId" = k."Id" 
WHERE c."CreatedAt" IS NOT NULL
GROUP BY
    CAST(a."CreatedAt" AS DATE),
    
    a."Id",
    a."OrderReturnCode",
    a."OrderPackageCode",
    "OrderReturnPackageCode",
    CAST(c."CreatedAt" AS DATE),
	c."CreatedAt",
    g."Name",
    h."Name",
    e."Name",
    "CustomerId",
    "OrderReferenceId",
    f."Name",
    "ReturnReasonDescription",
    i."Name",
    "IsFailedDelivery",
    "ProductId",
    "VariantId",
    "ItemPrice",
    "IsQC",
    j."Name",
    k."BranchName",
    "AccountName",
    "AccountNumber",
    "Amount",
    "IsReturnVoucher"
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
	created as (
	SELECT
		CAST("CreatedAt" AS DATE) AS date,
		COUNT(DISTINCT "OrderReferenceId") AS orders
	FROM public."vw_Orders"
	WHERE ("PaymentMode" NOT IN ('0') OR "PaymentStatus" IN ('1'))
	GROUP BY CAST("CreatedAt" AS DATE)
	),
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
		d."PackageCode",
		"IsCRMConfirm",
		"IsSellerConfirm",
		om."CustomerId",
		"CustomerName",
		"State" AS "ShippingState",
		"City" AS "ShippingCity",
		"Zone" AS "ShippingZone",
		"DestinationHub",
		om."SellerId",
		"SellerCode",
		"ShopName",
		"SourceHub",
		"SellerStatus",
		"LogisticsStatus",
		COALESCE(e."Name","CustomerStatus") AS "CustomerStatus",
		"SellerApprovalStatus",
		om."ProductId",
		om."VariantId",
		"ShopSKU",
		"ProductName",
		"VariantName",
		"CurrentStockQty",
		"MRP",
		"SKUStatus",
		"CategoryId",
		"Vertical",
		"cat1",
		"cat2",
		"cat3",
		"cat4",
		"cat5",
		"cat6",
		"deepestcategory",
		"WeightInKg",
		"SellingPrice",
		om."Quantity",
		"TotalPrice",
		"ShippingCost",
		"FreeShippinId",
		"CustomerShipping",
		"SellerShipping",
		"CartupShipping",
		"VoucherCode",
		"VoucherDiscount",
		"SellerDiscount",
		"CartupDiscount",
		"OrderValue",
		"PaymentMethod",
		"DiscountAmount",
		a."BinNumber",
		"CardLevel",
		"VoucherName",
		"VoucherDescription",
		"BankShare",
		"TransactionAmount",
		"IsPaid",
		"TransactionStatus",
		"TrxID",
		"CardNo",
		"FulfillmentBy",
		"CancelReason",
		"CancelReasonDescription",
		"LastUpdated",
		"OrderReturnCode",
		"OriginalPackageCode",
		"OrderReturnPackageCode",
		"CR_SourceHub",
		"CR_DesHub",
		"ReturnStatus",
		"ReturnReason",
		"ReturnReasonDescription",
		"PickUpCancelReason",
		"IsFailedDelivery",
		"ItemPrice",
		COALESCE(rq."total_returns",om."Quantity") AS "ReturnQty",
		"IsQC",
		"Bank" AS "RefundBank",
		"Branch" AS "RefundBranch",
		"AccountName" AS "RefundAccountName",
		"AccountNumber" AS "RefundAccountNumber",
		COALESCE("SellingPrice"*rq."total_returns"+"CustomerShipping"*rq."total_returns"/nullif(om."Quantity",0)-"VoucherDiscount"*rq."total_returns"/nullif(om."Quantity",0),"OrderValue") AS "RefundAmount",
		"IsReturnVoucher",
		"OrderDate",
		"CancelledDate",
		"DeliveredDate",
		"ReturnCreatedDate",
		"ReturnConfirmDate",
		"ReturnUpdatedDate",
		"ReturnPkgCreatedDate",
		CASE 
	        WHEN "IsPaid" = TRUE AND "CancelReason" IS NOT NULL AND "DeliveredDate" IS NULL AND "IsFailedDelivery" IS NULL
	            THEN TO_DATE("CancelledDate", 'YYYY-MM-DD')
			WHEN "IsFailedDelivery" = TRUE AND "PaymentMethod" NOT IN ('COD') 
	            THEN "ReturnCreatedDate" 
	        WHEN "IsFailedDelivery" = FALSE AND "IsQC" = TRUE 
	            THEN "ReturnPkgCreatedDate" 
	        ELSE NULL 
	    END AS "EligibleForRefund"
	FROM order_master om
	LEFT JOIN category_tree ct ON om."CategoryId" = ct."category_id"
	LEFT JOIN return_master rm ON om."CustomerOrderCode" = rm."OrderReferenceId" AND om."VariantId" = rm."VariantId"
	LEFT JOIN fwd_pkg d ON om."OrderId" = d."OrderId"
	LEFT JOIN crm ON COALESCE(d."PackageCode",om."CustomerOrderCode") = COALESCE(crm."OrderPackageCode",crm."CustomerOrderCode")
	LEFT JOIN return_qty rq ON om."CustomerOrderCode" = rq."OrderReferenceId" AND om."VariantId" = rq."VariantId"
	LEFT JOIN public."vw_OrderDiscounts" a ON om."CustomerOrderCode" = a."OrderReferenceId"
	LEFT JOIN public."vw_PrepaymentBankBins" b ON a."BinNumber" = b."BinNumber"
	LEFT JOIN public."vw_PrepaymentVouchers" c ON a."DiscountVoucherId" = c."Id"
	LEFT JOIN public."vw_PackageStatusInfos" e ON d."DeliveryStatusId" = e."Id"
WHERE "OrderDate" <= CURRENT_DATE
 ORDER BY 
	"OrderDate", om."CustomerOrderCode"