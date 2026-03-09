--Cartup Masterfile
WITH fwd_reverse AS(
	WITH ranked_transactions AS (
		WITH transactions AS (
			WITH FilteredData AS (
				SELECT
					*
				FROM
					"vw_Transactions"
				WHERE
					"TransactionStatus" IS NOT NULL
					AND "TransactionStatus" <> ''
			)
			SELECT
				*,
				ROW_NUMBER() OVER (
					PARTITION BY "OrderRefId"
					ORDER BY
						"TransactionStatus",
						"UpdatedAt" DESC
				) AS "Rank"
			FROM
				FilteredData
		)
		SELECT
			*
		FROM
			transactions
		WHERE
			"Rank" = '1'
	),
	order_master AS (
		SELECT
			CAST(a."CreatedAt" AS DATE) AS "OrderDate",
			"OrderReferenceId" AS "CustomerOrderCode",
			"OrderCode" AS "SellerOrderCode",
			r."PackageCode",
			CAST(r."CreatedAt" AS DATE) AS pkg_create,
			CAST(r."UpdatedAt" AS DATE) AS pkg_update,
			"IsCRMConfirm",
			"IsSellerConfirm",
			a."CustomerId",
			CONCAT(b."FirstName", ' ', b."LastName") AS "CustomerName",
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
			g."Price" * "Quantity" AS "TotalPrice",
			ROUND(
				"ActualShippingCost" * "WeightInKg" * "Quantity" / "TotalWeightInKg",
				2
			) as "ShippingCost",
			h."Id" AS "FreeShippinId",
			COALESCE(
				CASE
					WHEN "FreeShippingId" = 0 THEN ROUND(
						"ActualShippingCost" * "WeightInKg" * "Quantity" / "TotalWeightInKg",
						2
					)
				END,
				0
			) AS "CustomerShipping",
			ROUND(
				CASE
					WHEN h."IsFromAdmin" = 'true' THEN ROUND(
						"ActualShippingCost" * "WeightInKg" * "Quantity" / "TotalWeightInKg",
						2
					) * "SellerPercent" / 100
					WHEN h."IsFromAdmin" = 'false' THEN ROUND(
						"ActualShippingCost" * "WeightInKg" * "Quantity" / "TotalWeightInKg",
						2
					)
					ELSE 0
				END,
				2
			) AS "SellerShipping",
			ROUND(
				CASE
					WHEN h."IsFromAdmin" = 'true' THEN ROUND(
						"ActualShippingCost" * "WeightInKg" * "Quantity" / "TotalWeightInKg",
						2
					) * "AdminPercent" / 100
					ELSE 0
				END,
				2
			) AS "CartupShipping",
			a."VoucherCode",
			ROUND(
				"TotalDiscount" * g."Price" * "Quantity" / "TotalItemPrice",
				0
			) AS "VoucherDiscount",
			COALESCE(
				CASE
					WHEN i."SellerId" IS NOT NULL THEN ROUND(
						"TotalDiscount" * g."Price" * "Quantity" / "TotalItemPrice",
						0
					)
				END,
				0
			) AS "SellerDiscount",
			COALESCE(
				CASE
					WHEN i."SellerId" IS NULL THEN ROUND(
						"TotalDiscount" * g."Price" * "Quantity" / "TotalItemPrice",
						0
					)
				END,
				0
			) AS "CartupDiscount",
			g."Price" * "Quantity" + COALESCE(
				CASE
					WHEN "FreeShippingId" = 0 THEN ROUND(
						"ActualShippingCost" * "WeightInKg" * "Quantity" / "TotalWeightInKg",
						2
					)
				END,
				0
			) - ROUND(
				"TotalDiscount" * g."Price" * "Quantity" / "TotalItemPrice",
				0
			) AS "OrderValue",
			l."GateWayCode" AS "PaymentMethod",
			"Amount" AS "TransactionAmount",
			"IsPaid",
			"TransactionStatus",
			"TotalPackageWeightInKg",
			NULLIF(
				array_to_string(
					ARRAY_REMOVE(
						ARRAY [
					CASE WHEN "SslBankTranId" IS NOT NULL THEN "SslBankTranId" END,
					   CASE WHEN "BkashTrxId" IS NOT NULL THEN "BkashTrxId" END,
		               CASE WHEN "NagadPayerReference" IS NOT NULL THEN "NagadPayerReference" END,
		               CASE WHEN "UpayTrxId" IS NOT NULL THEN "UpayTrxId" END,
					   CASE WHEN "EblTrxUUID" IS NOT NULL THEN "EblTrxUUID" END
		           ],
						NULL
					),
					', '
				),
				''
			) AS "TrxID",
			NULLIF(
				array_to_string(
					ARRAY_REMOVE(
						ARRAY [
		               CASE WHEN "SslCardIssuer" IS NOT NULL THEN "SslCardIssuer" END,
					   CASE WHEN "BkashCustomerMsisdn" IS NOT NULL THEN "BkashCustomerMsisdn" END,
		               CASE WHEN "NagadCustomerMsisdn" IS NOT NULL THEN "NagadCustomerMsisdn" END,
		               CASE WHEN "UpayCustomerWallet" IS NOT NULL THEN "UpayCustomerWallet" END,
					   CASE WHEN "EblCardNo" IS NOT NULL THEN "EblCardNo" END
		           ],
						NULL
					),
					', '
				),
				''
			) AS "CardNo",
			CASE
				WHEN "FulfillmentBy" = 1 THEN 'MarketPlace'
				ELSE 'Retail'
			END AS "FulfillmentBy",
			CAST("DeliveredAt" AS DATE) AS "DeliveredAt",
			CAST("CancelledAt" AS DATE) AS "CancelledAt",
			q."Name" AS "CancelReason",
			"CancelReasonDescription",
			"JourneyType",
			CAST(a."UpdatedAt" AS DATE) AS "LastUpdated"
		FROM
			public."vw_Orders" a
			LEFT JOIN public."vw_Customers" b ON a."CustomerId" = b."Id"
			LEFT JOIN public."vw_Seller" c ON a."SellerId" = c."Id"
			LEFT JOIN public."vw_PackageStatusInfos" d ON a."PackageStatusId" = d."Id"
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
			LEFT JOIN public."vw_OrderPackages" r ON a."Id" = r."OrderId"AND a."SellerId" = r."SellerId"
			LEFT JOIN  public."vw_PackageJourneyType" s ON r."PackageCode" = s."PackageCode"
		WHERE r."PackageCode" IS not NULL
			
			and CAST(a."CreatedAt" AS DATE) >= '2024-08-28'
		ORDER BY
			"OrderDate",
			"OrderReferenceId"
	),
	category_tree AS (
		WITH "L1" AS (
			SELECT
				"Id",
				"IndustryId",
				"NameEn"
			FROM
				public."vw_Category"
			WHERE
				"ParentId" IS NULL
		),
		"L2" AS (
			SELECT
				L1."NameEn" AS cat1,
				COALESCE(L2."Id", L1."Id") AS "Id",
				COALESCE(L2."IndustryId", L1."IndustryId") AS "IndustryId",
				L2."NameEn" AS cat2
			FROM
				public."vw_Category" L2
				RIGHT JOIN "L1" L1 ON L1."Id" = L2."ParentId"
		),
		"L3" AS (
			SELECT
				cat1,
				cat2,
				COALESCE(L3."Id", L2."Id") AS "Id",
				COALESCE(L3."IndustryId", L2."IndustryId") AS "IndustryId",
				L3."NameEn" AS cat3
			FROM
				public."vw_Category" L3
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
			FROM
				public."vw_Category" L4
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
			FROM
				public."vw_Category" L5
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
			FROM
				public."vw_Category" L6
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
		FROM
			"L6" l
			LEFT JOIN public."vw_Industries" v ON l."vertical" = v."Id"
		ORDER BY
			"Vertical"
	),
	return_master AS (
		WITH ranked AS (
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
				CASE
					WHEN "IsFailedDelivery" = FALSE THEN ROW_NUMBER() OVER (
						PARTITION BY a."OrderReferenceId",
						"VariantId"
						ORDER BY
							a."Id" DESC,
							c."CreatedAt" DESC
					)
					ELSE '1'
				END AS "Rank"
			FROM
				public."vw_OrderReturns" a
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
		)
		SELECT
			*
		FROM
			ranked
		WHERE
			"Rank" = '1'
	),

	  crm_order AS (
		SELECT
			"CustomerOrderCode",
	MIN(CASE WHEN "LogisticStatus" = 'Pending' THEN  TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'), 'YYYY-MM-DD HH24:MI') END) AS "Pending",
    MIN(CASE WHEN "LogisticStatus" = 'CRM Confirmed' THEN   TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'), 'YYYY-MM-DD HH24:MI') END) AS "CRM Confirmed",
	MIN(CASE WHEN "SellerStatus" ='Pending For RTS'THEN  TO_CHAR(TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'), 'YYYY-MM-DD HH24:MI') END) AS "Pending For RTS"	
    FROM public."vw_OrderPackageTransactions"
		GROUP BY "CustomerOrderCode"
	),
	
	crm AS (
		SELECT
			"OrderPackageCode",
			 MIN(
				CASE 
				    WHEN "SellerStatus" = 'Ready To Ship' THEN  TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'), 
						'YYYY-MM-DD HH24:MI'
						)
					END
				) AS "RTS",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'Return Confirm' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "ReturnConfirmDate",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'In Pick-Up' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "In Pick-Up",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'First Pickup Attempt Failed' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "First Pickup Attempt",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'Second Pickup Attempt Failed' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "Second Pickup Attempt",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'Third Pickup Attempt Failed' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "Third Pickup Attempt",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'On the Way to Seller' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "On the Way to Seller",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'Pickup Successful' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "Pickup Successful",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'Waiting For Drop Off' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "Waiting For Drop Off",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'Dropped Off' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "Dropped Off",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'Pending At FM' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "Pending At FM",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'FM_MU_Packed' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "FM_MU_Packed",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'In Transit To Sort' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "In Transit To Sort",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'Pending At Sort' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "Pending At Sort",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'Sort_MU_Packed' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "Sort_MU_Packed",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'In Transit To LM' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "In Transit To LM",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'Pending At LM' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "Pending At LM",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'Assigned Delivery Man' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "Assigned Delivery Man",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'Received By Delivery Man' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "Received By Delivery Man",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'On The Way To Delivery' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "On The Way To Delivery",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'First Delivery Attempt Failed' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "First Delivery Attempt",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'Second Delivery Attempt Failed' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "Second Delivery Attempt",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'Third Delivery Attempt Failed' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "Third Delivery Attempt",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'Delivered' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "Delivered",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'Delivery Failed' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "Delivery Failed",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'LM_MU_Packed' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "LM_MU_Packed",
			MIN(CASE WHEN "LogisticStatus" = 'Canceled'THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI') END) AS "Canceled" ,

			MIN(
				CASE
					WHEN "LogisticStatus" = 'In Transit To FMR' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "In Transit To FMR",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'Pending At FMR' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "Pending At FMR",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'CR_Pickup Successful' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "CR_Pickup Successful",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'FMR MU Pack Created' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "FMR MU Pack Created",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'In Transit To FM' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "In Transit To FM",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'Delivery Man Assign' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "Delivery Man Assign",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'In Transit To FMQCC' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "In Transit To FMQCC",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'In Seller Delivery' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "In Seller Delivery",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'On The Way To Seller' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "On The Way To Seller",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'Returned to Seller' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "Returned to Seller",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'On The Way To Seller' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "On The Way To Seller",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'First Delivery Attempt Return Package' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "First Delivery Attempt Return Package",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'Second Delivery Attempt Return Package' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "Second Delivery Attempt Return Package",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'Third Delivery Attempt Return Package' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "Third Delivery Attempt Return Package",
			MIN(
				CASE
					WHEN "LogisticStatus" = 'Scrapped' THEN TO_CHAR(
						TO_TIMESTAMP("TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
						'YYYY-MM-DD HH24:MI'
					)
				END
			) AS "Scrapped"
		FROM
			public."vw_OrderPackageTransactions"
		GROUP BY
			"OrderPackageCode"
	)
	SELECT
		"PackageCode",
		"OrderReturnPackageCode",
		om."CustomerOrderCode",
		"SellerOrderCode",
		pkg_create as "CreatedAt",
		pkg_update as "UpdatedAt",
		"IsCRMConfirm",
		"IsSellerConfirm",
		om."CustomerId",
		"CustomerName",
		"State" AS "ShippingState",
		"City" AS "ShippingCity",
		"Zone" AS "ShippingZone",
		"DestinationHub",
		"SellerId",
		"SellerCode",
		"ShopName",
		"SellerStatus",
		"SourceHub",
		"LogisticsStatus",
		"CustomerStatus",
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
		"TotalPackageWeightInKg",
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
		"TransactionAmount",
		"IsPaid",
		"TransactionStatus",
		"TrxID",
		"CardNo",
		"FulfillmentBy",
		"DeliveredAt",
		"CancelledAt",
		"CancelReason",
		"CancelReasonDescription",
		"LastUpdated",
		"ReturnCreatedDate",
		"ReturnConfirmDate",
		"OrderReturnCode",
		"OriginalPackageCode",
		"ReturnPkgCreatedDate",
		"CR_SourceHub",
		"CR_DesHub",
		"ReturnStatus",
		"ReturnReason",
		"ReturnReasonDescription",
		"CR_Pickup Successful",
		"PickUpCancelReason",
		"IsFailedDelivery",
		"ItemPrice",
		rm."Quantity" AS "ReturnQty",
		"IsQC",
		rm."Bank" AS "RefundBank",
		rm."Branch" AS "RefundBranch",
		rm."AccountName" AS "RefundAccountName",
		rm."AccountNumber" AS "RefundAccountNumber",
		COALESCE(
			"SellingPrice" * rm."Quantity" + "CustomerShipping" * rm."Quantity" / om."Quantity" - "VoucherDiscount" * rm."Quantity" / om."Quantity",
			"OrderValue"
		) AS "RefundAmount",
		"IsReturnVoucher",
		"OrderDate",
		"CancelledAt",
		"LastUpdated",
		"ReturnCreatedDate",
		"ReturnConfirmDate",
		"JourneyType",
		"ReturnPkgCreatedDate",
		CASE
			WHEN "IsPaid" = TRUE
			AND "CancelReason" IS NOT NULL
			AND "DeliveredAt" IS NULL
			AND "IsFailedDelivery" IS NULL THEN COALESCE("CancelledAt", "LastUpdated")
			WHEN "IsFailedDelivery" = TRUE
			AND "PaymentMethod" NOT IN ('COD') THEN "ReturnCreatedDate"
			WHEN "IsFailedDelivery" = FALSE
			AND "IsQC" = TRUE THEN "ReturnPkgCreatedDate"
			ELSE NULL
		END AS "EligibleForRefund",
		"RTS",
		"In Pick-Up",
		"First Pickup Attempt",
		"Second Pickup Attempt",
		"Third Pickup Attempt",
		"On the Way to Seller",
		"Pickup Successful",
		"Waiting For Drop Off",
		"Dropped Off",
		"Pending At FM",
		"FM_MU_Packed",
		"In Transit To Sort",
		"Pending At Sort",
		"Sort_MU_Packed",
		"In Transit To LM",
		"Pending At LM",
		"Assigned Delivery Man",
		"Received By Delivery Man",
		"On The Way To Delivery",
		"First Delivery Attempt",
		"Second Delivery Attempt",
		"Third Delivery Attempt",
		"Delivered",
		"Canceled",
		"Delivery Failed",
		"LM_MU_Packed",
		"In Transit To FMR",
		"Pending At FMR",
		"FMR MU Pack Created",
		"In Transit To FM",
		"Delivery Man Assign",
		"In Seller Delivery",
		"First Delivery Attempt Return Package",
		"Second Delivery Attempt Return Package",
		"Third Delivery Attempt Return Package",
		"Returned to Seller",
		"Scrapped",
		"In Transit To FMQCC"
	FROM
		order_master om
		LEFT JOIN category_tree ct ON om."CategoryId" = ct."category_id"
		LEFT JOIN return_master rm ON om."CustomerOrderCode" = rm."OrderReferenceId"
		--AND om."VariantId" = rm."VariantId"
		LEFT JOIN crm_order CO ON om."CustomerOrderCode" = CO."CustomerOrderCode"
		LEFT JOIN crm ON om."PackageCode" = crm."OrderPackageCode"

	WHERE "OrderDate" >= '2024-08-28'
	ORDER BY
		"OrderDate",
		om."CustomerOrderCode"
) --------------------------		
SELECT
	"PackageCode" AS "package_code",
	"OrderReturnPackageCode" AS "order_return_packagecode",
	"CustomerOrderCode" AS "customer_order_code",
	"SellerOrderCode" AS "seller_order_code",
	CASE
		WHEN "Second Pickup Attempt" IS NOT NULL THEN '3'
		WHEN "First Pickup Attempt" IS NOT NULL THEN '2'
		WHEN "First Pickup Attempt" IS NULL THEN '1'
	END AS "pick_up_attempt_number",
	CASE
		WHEN "PaymentMethod" = 'COD' THEN 'COD'
		ELSE 'PrePayment'
	END AS "payment_type",
	CASE
		WHEN "Second Delivery Attempt" IS NOT NULL THEN '3'
		WHEN "First Delivery Attempt" IS NOT NULL THEN '2'
		WHEN "First Delivery Attempt" IS NULL THEN '1'
	END AS "delivery_attempt_number",

	"IsFailedDelivery" AS "is_failed_delivery",
	"Quantity" AS "quantity",
	"TotalPrice" AS "total_price",
	"SellerCode" AS "seller_code",
	"ShopName" AS "shop_name",
	"FulfillmentBy" AS "fulfillment_by",
	"TotalPackageWeightInKg" AS "total_package_weight_in_kg",
	"LogisticsStatus" AS "package_status",
	"SourceHub" AS "source_hub",
	"DestinationHub" AS "destination_hub",
	"CreatedAt" AS "created_at",
	"UpdatedAt" AS "updated_at",
	"In Pick-Up" AS "in_pick_up",
	"First Pickup Attempt" AS "first_pickup_attempt",
	"Second Pickup Attempt" AS "second_pickup_attempt",
	"Third Pickup Attempt" AS "third_pickup_attempt",
	"On the Way to Seller" AS "on_the_way_to_seller",
	"RTS" AS "rts",
	"Pickup Successful" AS "pickup_successful",
	"Waiting For Drop Off" AS "waiting_for_drop_off",
	"Dropped Off" AS "dropped_off",
    "Pending At FM" AS "pending_at_fm",
	"FM_MU_Packed" AS "fm_mu_packed",
	"In Transit To Sort" AS "in_transit_to_sort",
	"Pending At Sort" AS "pending_at_sort",
	"Sort_MU_Packed" AS "sort_mu_packed",
	"In Transit To LM" AS "in_transit_to_lm",
	"Pending At LM" AS "pending_at_lm",
	"Assigned Delivery Man" AS "assigned_delivery_man",
	"Received By Delivery Man" AS "received_by_delivery_man",
	"On The Way To Delivery" AS "on_the_way_to_delivery",
	"First Delivery Attempt" AS "first_delivery_attempt",
	"Second Delivery Attempt" AS "second_delivery_attempt",
	"Third Delivery Attempt" AS "third_delivery_attempt",
	"Delivered" AS "delivered",
	"Canceled" AS "canceled",
	"Delivery Failed" AS "delivery_failed",
	"LM_MU_Packed" AS "lm_mu_packed",
	"In Transit To FMR" AS "in_transit_to_fmr",
	"Pending At FMR" AS "pending_at_fmr",
	"FMR MU Pack Created" AS "fmr_mu_pack_created",
	"In Transit To FM" AS "in_transit_to_fm",
	"Delivery Man Assign" AS "delivery_man_assign",
	"In Seller Delivery" AS "in_seller_delivery",
	"First Delivery Attempt Return Package" AS "first_delivery_attempt_return_package",
	"Second Delivery Attempt Return Package" AS "second_delivery_attempt_return_package",
	"Third Delivery Attempt Return Package" AS "third_delivery_attempt_return_package",
	"Returned to Seller" AS "returned_to_seller",
	"Scrapped" AS "scrapped",
	"CR_Pickup Successful" AS "cr_pickup_successful",
	"In Transit To FMQCC" AS "in_transit_to_fmqcc",
	"IsQC" AS "is_qc",
	"JourneyType" AS "journey_type"
	FROM fwd_reverse 
	WHERE "PackageCode" IS NOT NULL;