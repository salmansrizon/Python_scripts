WITH order_master AS (
    SELECT
        DISTINCT ON (a."Id") a."Id" AS "OrderId",
        CAST(a."CreatedAt" AS DATE) AS "OrderDate",
        "OrderReferenceId" AS "CustomerOrderCode",
        "OrderCode" AS "OrderCode",
        "ShopName",
        CASE
            WHEN o."Name" IN (
                'Tongi Hub',
                'Uttara Hub',
                'Tejgaon Hub',
                'Badda Hub',
                'Tejgaon Sort Center',
                'Khilgaon Hub',
                'Mirpur Hub',
                'Jatrabari Hub',
                'Dhanmondi Hub',
                'Old Town hub',
                'Tejgaon Warehouse'
            ) THEN 'CartUp'
            WHEN o."Name" IN ('Agrabad Hub', '3PL', 'Patenga Hub', 'Probartak') THEN '3PL'
            ELSE 'Other'
        END AS "DestinationHub",
        OP."PackageCode",
        "TotalPackageWeightInKg",
        d."Name" as "PackageStatus",
        OP."SellerId",
        "SellerCode",
        TO_CHAR(a."CreatedAt", 'YYYY-MM-DD HH24:MI') AS "CreatedAt",
        TO_CHAR(a."UpdatedAt", 'YYYY-MM-DD HH24:MI') AS "UpdatedAt",
        "SellerType",
        "IsCRMConfirm",
        f."Name" AS "SellerStatus",
        "Price" * "Quantity" AS "TotalPrice",
        "Quantity",
        p."Name" AS "SourceHub",
        q."Name" AS "CancelReason",
        "CancelReasonDescription",
        j."NameEn" AS "ProductName",
        e."Name" AS "CustomerStatus",
        l."GateWayCode" AS "PaymentMethod",
        CASE
            WHEN a."FulfillmentBy" = 1 THEN 'MarketPlace'
            ELSE 'Retail'
        END AS "FulfillmentBy"
    FROM
        public."vw_Orders" a
        LEFT JOIN public."vw_Stores" o ON a."DestinationHubId" = o."Id"
        LEFT JOIN (
            SELECT
                DISTINCT ON ("OrderId") "OrderId",
                "PackageCode",
                "SellerId",
                "TotalPackageWeightInKg"
            FROM
                public."vw_OrderPackages"
        ) OP ON a."Id" = OP."OrderId"
        LEFT JOIN (
            SELECT
                DISTINCT ON ("OrderId") "OrderId",
                "ProductId",
                "Quantity",
                "Price"
            FROM
                public."vw_OrderItems"
        ) g ON a."Id" = g."OrderId"
        LEFT JOIN public."vw_Reasons" q ON a."CancelReasonId" = q."Id"
        LEFT JOIN public."vw_Seller" S ON A."SellerId" = S."Id"
        LEFT JOIN public."vw_PaymentMethods" l ON a."PaymentMode" = l."Id"
        LEFT JOIN public."vw_Product" j ON g."ProductId" = j."Id"
        LEFT JOIN public."vw_PackageStatusInfos" d ON a."PackageStatusId" = d."Id"
        LEFT JOIN public."vw_SellerDeliveryStatus" f ON d."SellerDeliveryStatusId" = f."Id"
        LEFT JOIN public."vw_CustomerDeliveryStatus" e ON d."CustomerDeliveryStatusId" = e."Id"
        LEFT JOIN public."vw_Stores" p ON a."SourceHubId" = p."Id"
),
return_master AS (
    WITH ranked AS (
        SELECT
            TO_CHAR(a."CreatedAt", 'YYYY-MM-DD HH24:MI') AS "ReturnCreatedDate",
            a."Id",
            a."OrderReturnCode",
            a."OrderPackageCode" as "OriginalPackageCode",
            c."OrderReturnPackageCode",
            CAST(c."CreatedAt" AS DATE) AS "ReturnPkgCreatedDate",
            e."Name" AS "ReturnStatus",
            "CustomerId",
            "OrderReferenceId",
            f."Name" AS "ReturnReason",
            "ReturnReasonDescription",
            "IsFailedDelivery",
            "ProductId",
            "VariantId",
            "Quantity",
			j."Name" AS "refund_method",
            "ItemPrice",
            c."IsQC",
            CASE
                WHEN "IsFailedDelivery" = FALSE THEN ROW_NUMBER() OVER (
                    PARTITION BY a."OrderReferenceId"
                    ORDER BY
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
            LEFT JOIN public."vw_OrderReturnPackages" RP ON a."Id" = RP."OrderReturnId"
			LEFT JOIN public."vw_Banks" j ON d."BankId" = j."Id"
                
			
			--WHERE c."CreatedAt" IS NOT NULL
            --WHERE c."OrderReturnPackageCode" IS NOT NULL
        GROUP BY
            TO_CHAR(a."CreatedAt", 'YYYY-MM-DD HH24:MI'),
            a."Id",
            a."OrderReturnCode",
            a."OrderPackageCode",
            c."OrderReturnPackageCode",
            CAST(c."CreatedAt" AS DATE),
            c."CreatedAt",
            e."Name",
            "CustomerId",
            "OrderReferenceId",
            f."Name",
            "ReturnReasonDescription",
            "IsFailedDelivery",
            "ProductId",
            "VariantId",
			j."Name",
            "Quantity",
            "ItemPrice",
            c."IsQC"
    )
    SELECT
        *
    FROM
        ranked
    WHERE
        "Rank" = '1'
),
Order_package AS (
    SELECT
        "OrderId",
        MIN(
            CASE
                WHEN "CustomerStatus" = 'Return Request' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "CR Initiated",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'CRM Confirmed' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "CRM Confirmed",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'CRM Rejected' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "CRM Rejected",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'CR Waiting For Pickup' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "CR Waiting For Pickup",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'CR_ In Pickup' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "CR In Pickup",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'CR_Pickup Successful' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "CR Pickup Successful",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'CR_Droped-Off' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "CR Droped Off",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'QC Passed' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "QC Passed",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'QC Rejected' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "QC Rejected",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'Delivery Failed' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "Delivery Failed",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'LM_MU_Packed' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "LM MU Packed",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'MU Pack Created' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "MU Pack Created",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'In Transit To FM' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "In Transit To FM",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'In Transit To Sort' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "In Transit To Sort",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'Pending At Sort' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "Pending At Sort",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'Pending' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "Pending",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'In Transit To FMR' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "In Transit To FMR",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'In Transit To FM QCC' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "In Transit To FM QCC",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'FMR MU Pack Created' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "FMR MU Pack Created",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'In Seller Delivery' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "In Seller Delivery",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'Returned to Seller' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "Returned to Seller",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'Return Confirm' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "CR Confirm",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'Sort_MU_Packed' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "Sort MU Packed",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'Waiting For RTM' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "Waiting For RTM",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'QCC_MU_Packed' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "QCC MU Packed",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'Pending At FMQCC' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "Pending At FMQCC",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'Pending At FMR' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "Pending At FMR",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'CR_Returned to Seller' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "CR Returned to Seller",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'In Transit To FMQCC' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "In Transit To FMQCC",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'First Return Pickup Attempt Failed' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "First Return Pickup Attempt Failed",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'Waiting For RTC' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "Waiting For RTC",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'Pending At LM' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "Pending At LM",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'Pending At FM' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "Pending At FM",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'In QC' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "In QC",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'Second Return Pickup Attempt Failed' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "Second Return Pickup Attempt Failed",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'Return Pickup Canceled' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "Return Pickup Canceled",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'First Delivery Attempt Return Package' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "First Delivery Attempt Return Package",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'Dropped Off' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "Dropped Off",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'In Transit' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "In Transit",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'In Transit To LM' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "In Transit To LM",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'Assigned Delivery Man' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "Assigned Delivery Man",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'Delivered' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "Delivered",
        MIN(
            CASE
                WHEN "CustomerStatus" = 'Return Successful' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "Return Successful",
        MIN(
            CASE
                WHEN "LogisticStatus" = 'Received By Delivery Man' THEN TO_CHAR(
                    TO_TIMESTAMP(OT."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                    'YYYY-MM-DD HH24:MI'
                )
            END
        ) AS "Received By Delivery Man"
    FROM
        public."vw_OrderPackageTransactions" OT
    GROUP BY
        "OrderId"
)
SELECT
    "PackageCode" as "package_code",
    "OriginalPackageCode" AS "original_package_code",
    "OrderReturnPackageCode" AS "order_return_package_code",
    OM."OrderId" AS "order_id",
    "CustomerOrderCode" AS "customer_order_code",
    "OrderCode" AS "order_code",
    "OrderReturnCode" AS "order_return_code",
    "SellerId" AS "seller_id",
    "SellerCode" AS "seller_code",
    "ShopName" AS "shop_name",
    "FulfillmentBy" AS "fulfillment_by",
    "ProductName" AS "product_name",
    rm."Quantity" AS "quantity",
    "TotalPrice" AS "total_price",
    "TotalPackageWeightInKg" AS "total_package_weight_in_kg",
    "SellerStatus" AS "seller_status",
    rm."ReturnStatus" AS "return_status",
    OM."CreatedAt" AS "order_created_at",
    OM."UpdatedAt" AS "order_updated_at",
	"Delivered" AS "delivered",
    "ReturnCreatedDate" AS "cr_initiated",
    "CR Confirm" AS "cr_confirm",
    "CRM Rejected" AS "crm_rejected",
    "CR Waiting For Pickup" AS "cr_waiting_for_pickup",
    "CR In Pickup" AS "cr_in_pickup",
    "First Return Pickup Attempt Failed" AS "first_return_pickup_attempt_failed",
    "Second Return Pickup Attempt Failed" AS "second_return_pickup_attempt_failed",
    "CR Pickup Successful" AS "cr_pickup_successful",
    "Return Pickup Canceled" AS "return_pickup_canceled",
    "CR Droped Off" AS "cr_droped_off",
	"Pending At LM" AS "pending_at_lm",
	"In Transit To FM QCC" AS "in_transit_to_fm_qcc",
    "In Transit To FMQCC" AS "in_transit_to_fmqcc",
    "Pending At FMQCC" AS "pending_at_fmqcc", 
    "In QC" AS "in_qc",
    "QC Passed" AS "qc_passed",
    "QC Rejected" AS "qc_rejected",
	"Waiting For RTM" AS "waiting_for_rtm",
	"Waiting For RTC" AS "waiting_for_rtc",
    "QCC MU Packed" AS "qcc_mu_packed",
    "In Transit To FMR" AS "in_transit_to_fmr",
	"FMR MU Pack Created" AS "fmr_mu_pack_created",
    "Pending At FMR" AS "pending_at_fmr",
    "In Transit To FM" AS "in_transit_to_fm",
	"Pending At FM" AS "pending_at_fm",
    "In Seller Delivery" AS "in_seller_delivery",
    "CR Returned to Seller" AS "cr_returned_to_seller",
    "Return Successful" AS "return_successful",
    "SourceHub" AS "source_hub",
    OM."DestinationHub" AS "destination_hub",
    "PaymentMethod" AS "payment_method",
	"refund_method",
    "ReturnReason" AS "return_reason",
    rm."ReturnReasonDescription" AS "return_reason_description"
FROM
    return_master rm
    LEFT JOIN order_master OM ON rm."OriginalPackageCode" = OM."PackageCode"
    LEFT JOIN Order_package OP ON OM."OrderId" = OP."OrderId"
WHERE
    "OrderDate" >= '2024-08-28'
    AND "SellerId" NOT IN ('1', '3')
    AND (
        "IsCRMConfirm" = TRUE
        OR "CustomerStatus" = 'Pending'
    )
    AND (
        "CancelReason" IS NULL
        OR "CancelReason" ILIKE '%test%'
    )
    AND (
        "CancelReasonDescription" IS NULL
        OR "CancelReasonDescription" LIKE '%test%'
    )
    AND "ProductName" NOT ILIKE '%not for sale%'
    AND "PaymentMethod" IS NOT NULL
    AND "OrderReturnCode" IS NOT NULL
    AND "IsFailedDelivery" = 'FALSE'
