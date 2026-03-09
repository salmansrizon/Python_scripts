WITH ranked_transactions AS (
	WITH transactions AS (
		WITH FilteredData AS (
			SELECT *, m."UpdatedAt" AS "RankingDate"
			FROM public."vw_Transactions_cartup" m
					WHERE m."TransactionStatus" IS NOT NULL 
					AND m."TransactionStatus" <> ''
		)
		SELECT 
			*, 
			ROW_NUMBER() OVER (
				PARTITION BY "OrderRefId" 
				ORDER BY "TransactionStatus", "RankingDate" DESC  ) AS "Rank"
		FROM FilteredData
	)
	SELECT * 
	FROM transactions
	WHERE "Rank" = '1'
)
 

,order_master AS (
	SELECT
		CAST(a."CreatedAt" AS DATE) AS "OrderDate",
		EXTRACT(HOUR FROM a."CreatedAt")   AS "Hour",
		EXTRACT(MONTH FROM a."CreatedAt")  AS "Month",
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
		a."SourceHubId",
        a."DestinationHubId",
		o."Name" AS "DestinationHub",
		a."SellerId",
		"SellerCode",
		"ShopName",
        CASE WHEN "IsDropToHub" = 'true' THEN 'Drop-off Seller' ELSE 'Pickup Seller' END AS "Seller Type",
        p."Name" AS "SourceHub",
    	d."Name" AS "OldLogisticsStatus",
        ls."LatestLogisticStatus" AS "LogisticStatus",
		e."Name" AS "CustomerStatus",
		f."Name" AS "SellerStatus",
		g."ProductId",
		g."VariantId",
		g."ShopSKU",	
		"IsConsignmentSku",
		j."NameEn" AS "ProductName",
		k."Name" AS "VariantName",
		g."CategoryId",
		"WeightInKg",
		k."CurrentStockQty",
		k."Price" AS "MRP",
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
		a."VoucherId",
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
		CASE WHEN g."IsConsignment" IS TRUE THEN 'Consignment'
			 WHEN "ShopName" IN ('Cartup Retail') THEN 'Retail' ELSE 'MarketPlace' END AS "FulfillmentBy",
		q."Name" AS "CancelReason",
		"CancelReasonDescription",
		CAST(a."UpdatedAt" AS DATE) AS "LastUpdated",
		s."Packageid",
		t."PickUpRiderId",
		u.rider_name AS "Pickup Rider Name",
         u.full_name AS "Pickup Rider Full Name",
		u.rider_phone AS "Pickup Rider Phone",
		u.rider_status AS "Pickup Rider Status",
		t."DeliveryRiderId",
		v.rider_name AS "Delivery Rider Name",
        v.full_name AS "Delivery Rider Full Name",
		v.rider_phone AS "Delivery Rider Phone",
		v.rider_status AS "Delivery Rider Status",
		"PackageReadyTime",
        "RiderPickupTime",
     	"ActualDeliveryTime",
		"FailedDeliverReason",
       --  s."State",
		-- s."City",
		-- s."Zone",
		s."PackageStatus",
		s."PickupCancelReason",
        n."Name" as "Receiver Name",
		n."Address" as "Receiver Address",
		n."Phone" as "Receiver Phone",
		a."RequestedDeviceId"
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
		LEFT JOIN public."vw_OrderPackages" t ON g."OrderId" = t."OrderId"
        LEFT JOIN (
        	SELECT DISTINCT ON ("SellerOrderCode")
        		"SellerOrderCode",
        		"LogisticStatus" AS "LatestLogisticStatus"
       		FROM public."vw_OrderPackageTransactions"
        	WHERE "LogisticStatus" IS NOT NULL
        	ORDER BY "SellerOrderCode", to_timestamp("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM') DESC) ls ON a."OrderCode" = ls."SellerOrderCode"

	    LEFT JOIN (
        	SELECT DISTINCT ON ("Packageid") *
        	FROM public.mv_package_info
        	ORDER BY "Packageid", ctid DESC ) s ON t."Id" = s."Packageid"
	    --LEFT JOIN public.mv_package_info s ON t."Id" = s."Packageid"
		LEFT JOIN public.mv_rider_info u ON t."PickUpRiderId" = u.rider_id
		LEFT JOIN public.mv_rider_info v ON t."DeliveryRiderId" = v.rider_id
    	WHERE 1=1  
        AND CAST(a."CreatedAt" AS DATE) between '2025-02-01' and CURRENT_DATE
		ORDER BY "OrderDate","OrderReferenceId"
)
	
,category_tree AS (

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
)

,return_master AS (
	WITH ranked AS (
		SELECT
			CAST(a."CreatedAt" AS DATE) AS "ReturnCreatedDate",
			CAST(a."UpdatedAt" AS DATE) AS "ReturnUpdatedDate",
			a."Id",
			a."RiderId" as "ReturnPickupRiderId",
			c."RiderId" as "ReturnDeliveryRiderId",
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
			i."Name" AS "ReturnPickUpCancelReason",
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
			a."RiderId",
			c."RiderId",
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
	)
	SELECT * FROM ranked
	WHERE "Rank" = '1'
)

,crm_order AS (
	SELECT "CustomerOrderCode",
	MIN(CASE WHEN "LogisticStatus" = 'Pending' THEN  TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'), 'YYYY-MM-DD HH24:MI') END) AS "Pending",
    MIN(CASE WHEN "LogisticStatus" = 'CRM Confirmed' THEN   TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'), 'YYYY-MM-DD HH24:MI') END) AS "CRM Confirmed",
	MIN(CASE WHEN "SellerStatus" ='Pending For RTS'THEN  TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'), 'YYYY-MM-DD HH24:MI') END) AS "Pending For RTS"	
    FROM public."vw_OrderPackageTransactions"
	GROUP BY "CustomerOrderCode"
)

,crm AS (
	SELECT
		"CustomerOrderCode",
		"OrderPackageCode",
		MIN(CASE WHEN "LogisticStatus" = 'Canceled' THEN 
        TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS'), 'YYYY-MM-DD') END) AS "CancelledDate",
		MIN(CASE WHEN "LogisticStatus" = 'Delivered' THEN 
        TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS'), 'YYYY-MM-DD') END) AS "DeliveredDate",
		MIN(CASE WHEN "LogisticStatus" = 'Return Confirm' THEN 
        TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS'), 'YYYY-MM-DD') END) AS "ReturnConfirmDate",
		MIN(CASE WHEN "SellerStatus" = 'Ready To Ship' THEN  TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),'YYYY-MM-DD HH24:MI') END) AS "RTS",				
		MIN(CASE WHEN "LogisticStatus" = 'In Pick-Up' THEN  TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'), 'YYYY-MM-DD HH24:MI') END) AS "In Pick-Up",
	
        MIN(CASE WHEN "LogisticStatus" = 'First Pick-Up Attempt Failed' THEN  TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'), 'YYYY-MM-DD HH24:MI') END) AS "First Pickup Attempt",
		MIN(CASE WHEN "LogisticStatus" = 'Second Pick-Up Attempt Failed' THEN  TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'), 'YYYY-MM-DD HH24:MI') END) AS "Second Pickup Attempt",
		MIN(CASE WHEN "LogisticStatus" = 'Third Pick-Up Attempt Failed' THEN  TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'), 'YYYY-MM-DD HH24:MI') END) AS "Third Pickup Attempt",
		MIN(CASE WHEN "LogisticStatus" = 'Pickup Successful' THEN  TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'), 'YYYY-MM-DD HH24:MI') END) AS "Pickup Successful",
		MIN(CASE WHEN "LogisticStatus" = 'Pending At FM' THEN  TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'), 'YYYY-MM-DD HH24:MI') END) AS "Pending At FM",
		MIN(CASE WHEN "LogisticStatus" = 'Pending At Sort' THEN  TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'), 'YYYY-MM-DD HH24:MI') END) AS "Pending At Sort",
		MIN(CASE WHEN "LogisticStatus" = 'Pending At LM' THEN  TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'), 'YYYY-MM-DD HH24:MI') END) AS "Pending At LM",
		MIN(CASE WHEN "LogisticStatus" = 'Assigned Delivery Man' THEN  TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'), 'YYYY-MM-DD HH24:MI') END) AS "Assigned Delivery Man",
		MIN(CASE WHEN "LogisticStatus" = 'First Delivery Attempt Failed' THEN  TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'), 'YYYY-MM-DD HH24:MI') END) AS "First Delivery Attempt",
		MIN(CASE WHEN "LogisticStatus" = 'Second Delivery Attempt Failed' THEN  TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'), 'YYYY-MM-DD HH24:MI') END) AS "Second Delivery Attempt",
		MIN(CASE WHEN "LogisticStatus" = 'Third Delivery Attempt Failed' THEN  TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'), 'YYYY-MM-DD HH24:MI') END) AS "Third Delivery Attempt",
		MIN(CASE WHEN "LogisticStatus" = 'Delivered' THEN  TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'), 'YYYY-MM-DD HH24:MI') END) AS "Delivered",
		MIN(CASE WHEN "LogisticStatus" = 'Delivery Failed' THEN  TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'), 'YYYY-MM-DD HH24:MI') END) AS "Delivery Failed"
	FROM public."vw_OrderPackageTransactions"
	GROUP BY "CustomerOrderCode", "OrderPackageCode"
	HAVING COALESCE("OrderPackageCode",MIN(CASE WHEN "LogisticStatus" = 'Canceled' THEN 
        TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS'), 'YYYY-MM-DD') END),MIN(CASE WHEN "LogisticStatus" = 'Delivered' THEN 
        TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS'), 'YYYY-MM-DD') END),MIN(CASE WHEN "LogisticStatus" = 'Return Confirm' THEN 
        TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS'), 'YYYY-MM-DD') END)) IS NOT NULL
)

,return_journey AS (
    SELECT
        "SellerOrderCode",
        MIN(CASE WHEN "LogisticStatus" = 'Return Confirm' THEN 
            TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'), 'YYYY-MM-DD HH24:MI') END) AS "Return Confirm",
       MIN(CASE WHEN "LogisticStatus" = 'CR_ In Pickup' THEN 
            TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'), 'YYYY-MM-D HH24:MI') END) AS "CR_ In Pickup",
	   MIN(CASE WHEN "LogisticStatus" = 'CR_Pickup Successful' THEN 
            TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'), 'YYYY-MM-DD HH24:MI') END) AS "CR_Pickup Successful",
        MIN(CASE WHEN "LogisticStatus" = 'First Return Pickup Attempt Failed' THEN 
            TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'), 'YYYY-MM-DD HH24:MI') END) AS "First Return Pickup Attempt",
        MIN(CASE WHEN "LogisticStatus" = 'Second Return Pickup Attempt Failed' THEN 
            TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'), 'YYYY-MM-DD HH24:MI') END) AS "Second Return Pickup Attempt",
        MIN(CASE WHEN "LogisticStatus" = 'Cancel Return Package' THEN 
            TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'), 'YYYY-MM-DD HH24:MI') END) AS "Cancel Return Package",
        MIN(CASE WHEN "LogisticStatus" = 'Return Pickup Canceled' THEN 
            TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'), 'YYYY-MM-DD HH24:MI') END) AS "Return Pickup Canceled",
        MIN(CASE WHEN "LogisticStatus" = 'Delivery Man Assign' THEN 
            TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'), 'YYYY-MM-DD HH24:MI') END) AS "Delivery Man Assign",
        MIN(CASE WHEN "LogisticStatus" = 'First Delivery Attempt Return Package' THEN 
            TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'), 'YYYY-MM-DD HH24:MI') END) AS "First Delivery Attempt Return Package",
        MIN(CASE WHEN "LogisticStatus" = 'Second Delivery Attempt Return Package' THEN 
            TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'), 'YYYY-MM-DD HH24:MI') END) AS "Second Delivery Attempt Return Package",
        MIN(CASE WHEN "LogisticStatus" = 'Third Delivery Attempt Return Package' THEN 
            TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'), 'YYYY-MM-DD HH24:MI') END) AS "Third Delivery Attempt Return Package",
        MIN(CASE WHEN "LogisticStatus" = 'Fourth Delivery Attempt Return Package' THEN 
            TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'), 'YYYY-MM-DD HH24:MI') END) AS "Fourth Delivery Attempt Return Package",
        -- combined version: Returned to Seller or CR_Returned to Seller
        COALESCE(
            MIN(CASE WHEN "LogisticStatus" = 'Returned to Seller' THEN 
                TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'), 'YYYY-MM-DD HH24:MI') END),
            MIN(CASE WHEN "LogisticStatus" = 'CR_Returned to Seller' THEN 
                TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'), 'YYYY-MM-DD HH24:MI') END)
        ) AS "Returned to Seller"
    FROM public."vw_OrderPackageTransactions"
    GROUP BY "SellerOrderCode"
)

		
,return_qty AS (
		WITH returned AS (
			SELECT
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
				i."Name" AS "ReturnPickUpCancelReason",
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
			ORDER BY a."OrderReferenceId","VariantId"
			)
	SELECT
		"OrderReferenceId",
		"VariantId",
		SUM("Quantity") AS total_returns
	FROM returned
	WHERE "IsFailedDelivery" IS FALSE
	AND "IsQC" IS TRUE
	GROUP BY "OrderReferenceId",
		"VariantId"
)

,created as (
		SELECT
			CAST("CreatedAt" AS DATE) AS date,
			COUNT(DISTINCT "OrderReferenceId") AS orders
		FROM public."vw_Orders"
		WHERE ("PaymentMode" NOT IN ('0') OR "PaymentStatus" IN ('1'))
		GROUP BY CAST("CreatedAt" AS DATE)
)

,all_pkg AS (
    SELECT
        "SellerOrderCode" AS pkg_seller_code,
        "OrderPackageCode" AS "PackageCode"
    FROM public."vw_OrderPackageTransactions"
    WHERE "OrderPackageCode" IS NOT NULL
    GROUP BY
        "SellerOrderCode",
        "OrderPackageCode"
)

,fwd_pkg AS (
	SELECT
		"OrderId",
		"PackageCode",
		"SourceHubId",
		"DeliveryStatusId"
	FROM "vw_OrderPackages"
	WHERE "IsQC" IS FALSE
)



SELECT DISTINCT
		"OrderDate",
		"Month",
		"Hour",
		"Pending" as "OrderTimestamp",
		om."CustomerOrderCode",
		om."SellerOrderCode",
		ap."PackageCode" AS "PackageCode",
		-- "Packageid",
		 "OrderReturnPackageCode",
		"IsCRMConfirm",
    	"IsSellerConfirm",
		"IsFailedDelivery",
		"ItemPrice",
--		LEAST(COALESCE(rq."total_returns",om."Quantity"),om."Quantity") AS "ReturnQty",
		"IsQC",
		om."CustomerId",
		"CustomerName",
		"Receiver Name",
		"Receiver Address",
		"Receiver Phone",
		"RequestedDeviceId",
		"State" AS "ShippingState",
		"City" AS "ShippingCity",
		"Zone" AS "ShippingZone",
		"SourceHub",
		"DestinationHub",
		-- rr_pickup_hub."Name" AS "Return Pickup Hub",
  --       rr_delivery_hub."Name" AS "Return Delivery Hub",
		om."SellerId",
		"SellerCode",
		"ShopName",
		"Seller Type",		
		"SellerStatus",
--       "OldLogisticsStatus",
		 "LogisticStatus",
  --       "PackageStatus",
         "ReturnStatus",
		COALESCE(e."Name","CustomerStatus") AS "CustomerStatus",
		CASE WHEN "Second Pickup Attempt" IS NOT NULL THEN '3'
			 WHEN "First Pickup Attempt" IS NOT NULL THEN '2'
			 WHEN "First Pickup Attempt" IS NULL THEN '1' END AS "PickUp Attempt Number",
		CASE WHEN "Second Delivery Attempt" IS NOT NULL THEN '3'
			 WHEN "First Delivery Attempt" IS NOT NULL THEN '2'
			 WHEN "First Delivery Attempt" IS NULL THEN '1' END AS "Delivery Attempt Number",
		-- CASE 
  --            WHEN rj."Second Return Pickup Attempt" IS NOT NULL THEN '3'
  --            WHEN rj."First Return Pickup Attempt" IS NOT NULL THEN '2'
  --            WHEN rj."First Return Pickup Attempt" IS NULL THEN '1' END AS "Return Pickup Attempt Number",

  --       CASE 
  --            WHEN rj."Fourth Delivery Attempt Return Package" IS NOT NULL THEN '5'
  --            WHEN rj."Third Delivery Attempt Return Package" IS NOT NULL THEN '4'
  --            WHEN rj."Second Delivery Attempt Return Package" IS NOT NULL THEN '3'
  --            WHEN rj."First Delivery Attempt Return Package" IS NOT NULL THEN '2'
  --            WHEN rj."First Delivery Attempt Return Package" IS NULL THEN '1'END AS "Return Delivery Attempt Number",

		om."ProductId",
		om."VariantId",
		"ShopSKU",
		"ProductName",
		"VariantName",
		"CurrentStockQty",
--		"MRP",
--		"CategoryId",
		"Vertical",
		"cat1",
--		"cat2",
--		"cat3",
--		"cat4",
--		"cat5",
--		"cat6",
		"deepestcategory",
--		"WeightInKg",
--		"SellingPrice",
		om."Quantity",
		"TotalPrice",
		"ShippingCost",
		"FreeShippinId",
		-- "CustomerShipping",
		-- "SellerShipping",
		-- "CartupShipping",
		"VoucherCode",
		"VoucherId",
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
		"FailedDeliverReason",
		"ReturnReason",
		"ReturnReasonDescription",
		"PickupCancelReason",
		"ReturnPickUpCancelReason",
		"CancelReasonDescription",
		"LastUpdated",
		"OrderReturnCode",
		"OriginalPackageCode",
		
--		"CR_SourceHub",
--		"CR_DesHub",
       
		
		
--		"ItemPrice",

		LEAST(COALESCE(rq."total_returns",om."Quantity"),om."Quantity") AS "ReturnQty",
		
--		"Bank" AS "RefundBank",
--		"Branch" AS "RefundBranch",
--		"AccountName" AS "RefundAccountName",
--		"AccountNumber" AS "RefundAccountNumber",
--		COALESCE("SellingPrice"*rq."total_returns"+"CustomerShipping"*rq."total_returns"/nullif(om."Quantity",0)-"VoucherDiscount"*rq."total_returns"/nullif(om."Quantity",0),"OrderValue") AS "RefundAmount",
--		"IsReturnVoucher",
		
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
	    END AS "EligibleForRefund",
				"Pending",
			-- "CRM Confirmed",	
			-- "RTS",	
   --          "In Pick-Up",
   --          "First Pickup Attempt",
   --          "Second Pickup Attempt",
   --          "Third Pickup Attempt",
			-- "Pickup Successful",
			-- "Pending At FM",
			-- "Pending At Sort",
			-- "Pending At LM",
			-- "Assigned Delivery Man",
   --          "First Delivery Attempt",
			-- "Second Delivery Attempt",
			-- "Third Delivery Attempt",
			-- "Delivered",
			-- "Delivery Failed",
			-- rj."Return Confirm",
			-- rj."CR_ In Pickup",
   --          rj."CR_Pickup Successful",
   --          rj."First Return Pickup Attempt",
   --          rj."Second Return Pickup Attempt",
   --          rj."Cancel Return Package",
   --          rj."Return Pickup Canceled",
   --          rj."Delivery Man Assign",
   --          rj."First Delivery Attempt Return Package",
   --          rj."Second Delivery Attempt Return Package",
   --          rj."Third Delivery Attempt Return Package",
   --          rj."Fourth Delivery Attempt Return Package",
   --          rj."Returned to Seller",

--	CASE WHEN "Delivered" IS NOT NULL AND "Pending" IS NOT NULL
--         THEN EXTRACT(EPOCH FROM ("Delivered"::timestamp - "Pending"::timestamp)) / 3600.0
--    END AS "Pending to Delivered",

--    CASE WHEN "Delivered" IS NOT NULL AND "CRM Confirmed" IS NOT NULL
--         THEN EXTRACT(EPOCH FROM ("Delivered"::timestamp - "CRM Confirmed"::timestamp)) / 3600.0
--    END AS "CRM Confirmed to Delivered",

--    CASE WHEN "Delivered" IS NOT NULL AND "RTS" IS NOT NULL
--         THEN EXTRACT(EPOCH FROM ("Delivered"::timestamp - "RTS"::timestamp)) / 3600.0
--    END AS "RTS to Delivered",

--    CASE WHEN "Delivered" IS NOT NULL AND "Pending At FM" IS NOT NULL
--         THEN EXTRACT(EPOCH FROM ("Delivered"::timestamp - "Pending At FM"::timestamp)) / 3600.0
--    END AS "Shipped to Delivered",

--    CASE WHEN "CRM Confirmed" IS NOT NULL AND "Pending" IS NOT NULL
--         THEN EXTRACT(EPOCH FROM ("CRM Confirmed"::timestamp - "Pending"::timestamp)) / 3600.0
--    END AS "Pending to Confirmed",

--    CASE WHEN "RTS" IS NOT NULL AND "CRM Confirmed" IS NOT NULL
 --        THEN EXTRACT(EPOCH FROM ("RTS"::timestamp - "CRM Confirmed"::timestamp)) / 3600.0
--    END AS "Confirmed to RTS",

--    CASE WHEN "Pending At FM" IS NOT NULL AND "RTS" IS NOT NULL
--         THEN EXTRACT(EPOCH FROM ("Pending At FM"::timestamp - "RTS"::timestamp)) / 3600.0
--    END AS "RTS to Shipped",

--    CASE WHEN "Pending At Sort" IS NOT NULL AND "Pending At FM" IS NOT NULL
--         THEN EXTRACT(EPOCH FROM ("Pending At Sort"::timestamp - "Pending At FM"::timestamp)) / 3600.0
--    END AS "Shipped to Sort",

--    CASE WHEN "Assigned Delivery Man" IS NOT NULL AND "Pending At Sort" IS NOT NULL
--         THEN EXTRACT(EPOCH FROM ("Assigned Delivery Man"::timestamp - "Pending At Sort"::timestamp)) / 3600.0
--    END AS "Sort to LastMile",

--    CASE WHEN "Delivered" IS NOT NULL AND "Assigned Delivery Man" IS NOT NULL
--         THEN EXTRACT(EPOCH FROM ("Delivered"::timestamp - "Assigned Delivery Man"::timestamp)) / 3600.0
--    END AS "LastMile to Delivered",
	
	
	"PickUpRiderId",
	-- "Pickup Rider Name",
 --    om."Pickup Rider Full Name",
--	"Pickup Rider Phone",
	--"Pickup Rider Status",
	"DeliveryRiderId",
	"Delivery Rider Name",
    om."Delivery Rider Full Name",
	"Delivery Rider Phone",
	"Delivery Rider Status",
   -- "ReturnPickupRiderId",
	"ReturnDeliveryRiderId",
	-- rr_pickup.rider_name AS "Return Pickup Rider Name",
 --    rr_pickup.full_name AS "Return Pickup Rider Full Name",
    rr_delivery.rider_name AS "Return Delivery Rider Name",
    rr_delivery.full_name AS "Return Delivery Rider Full Name"
        


    
	FROM order_master om
	LEFT JOIN category_tree ct ON om."CategoryId" = ct."category_id"
	LEFT JOIN return_master rm ON om."CustomerOrderCode" = rm."OrderReferenceId" AND om."VariantId" = rm."VariantId"
	LEFT JOIN public.mv_rider_info rr_pickup ON rm."ReturnPickupRiderId" = rr_pickup.rider_id
    LEFT JOIN public.mv_rider_info rr_delivery ON rm."ReturnDeliveryRiderId" = rr_delivery.rider_id
   LEFT JOIN public."vw_Stores" rr_pickup_hub  ON om."DestinationHubId" = rr_pickup_hub."Id"
   LEFT JOIN public."vw_Stores" rr_delivery_hub  ON om."SourceHubId" = rr_delivery_hub."Id"

   LEFT JOIN fwd_pkg d ON om."OrderId" = d."OrderId" AND om."SourceHubId" = d."SourceHubId"
	LEFT JOIN all_pkg ap ON om."SellerOrderCode" = ap.pkg_seller_code

	LEFT JOIN crm_order CO ON om."CustomerOrderCode" = CO."CustomerOrderCode"
	LEFT JOIN crm ON d."PackageCode" = crm."OrderPackageCode"
	LEFT JOIN return_qty rq ON om."CustomerOrderCode" = rq."OrderReferenceId" AND om."VariantId" = rq."VariantId"
	LEFT JOIN return_journey rj ON om."SellerOrderCode" = rj."SellerOrderCode"
	LEFT JOIN public."vw_OrderDiscounts" a ON om."CustomerOrderCode" = a."OrderReferenceId"
	LEFT JOIN public."vw_PrepaymentBankBins" b ON a."BinNumber" = b."BinNumber"
	LEFT JOIN public."vw_PrepaymentVouchers" c ON a."DiscountVoucherId" = c."Id"
	LEFT JOIN public."vw_PackageStatusInfos" e ON d."DeliveryStatusId" = e."Id"
    WHERE "OrderDate" <= CURRENT_DATE
    
ORDER BY 
	"OrderDate", om."CustomerOrderCode"