WITH pkg_status AS (
    SELECT
        "PackageCode",
        CASE
            WHEN TO_CHAR(a."UpdatedAt", 'YYYY-MM-DD HH24:MI') = TO_CHAR(
                TO_TIMESTAMP(h."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                'YYYY-MM-DD HH24:MI'
            ) THEN "LogisticStatus"
        END AS "PackageStatus",
        ROW_NUMBER() OVER (
            PARTITION BY "PackageCode"
            ORDER BY
                CASE
                    WHEN TO_CHAR(a."UpdatedAt", 'YYYY-MM-DD HH24:MI') = TO_CHAR(
                        TO_TIMESTAMP(h."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                        'YYYY-MM-DD HH24:MI'
                    ) THEN "LogisticStatus"
                END
        ) AS "Rank"
    FROM
        public."vw_OrderPackages" a
        LEFT JOIN public."vw_OrderPackageTransactions" h ON a."PackageCode" = h."OrderPackageCode"
    WHERE
        CASE
            WHEN TO_CHAR(a."UpdatedAt", 'YYYY-MM-DD HH24:MI') = TO_CHAR(
                TO_TIMESTAMP(h."TransactionTime", 'DD/MM/YYYY HH12:MI:SS AM'),
                'YYYY-MM-DD HH24:MI'
            ) THEN "LogisticStatus"
        END IN ('Assigned Delivery Man', 'Pending At LM')
)
SELECT
    a."PackageCode",
    a."PackageCode" AS "OriginalPackageCode",
    "OrderReferenceId" AS "OrderNo",
    "OrderCode" AS "OrderCode",
    "PackedItemQuantity" AS "Quantity",
    ROUND("PackedItemAmount" :: NUMERIC, 0) AS "TotalPrice",
    "TotalPackageWeightInKg",
    CASE
        WHEN c."IsDropToHub" iS TRUE THEN 'Drop-Off'
        ELSE 'Pickup'
    END AS "SellerType",
    c."SellerCode",
    CONCAT(c."FirstName", ' ', c."LastName") AS "SellerName",
    c."ShopName",
    CASE
        WHEN a."PackageCode" ILIKE 'pkg%' THEN 'MarketPlace'
        WHEN a."PackageCode" ILIKE 'opc%' THEN 'Warehouse'
    END AS "FulfillmentCode",
    'Sales Order' AS "OrderType",
    'Forward' AS "JourneyType",
    "PackageStatus",
    e."Name" AS "SourceHub",
    f."Name" AS "DestinationHub",
    g."Name" AS "CurrentLocation",
    TO_CHAR(a."CreatedAt", 'YYYY-MM-DD HH24:MI') AS "CreatedAt",
    TO_CHAR(a."UpdatedAt", 'YYYY-MM-DD HH24:MI') AS "UpdatedAt"
FROM
    public."vw_OrderPackages" a
    LEFT JOIN public."vw_Orders" b ON a."OrderId" = b."Id"
    LEFT JOIN public."vw_Seller" c ON a."SellerId" = c."Id"
    LEFT JOIN public."vw_Stores" e ON a."SourceHubId" = e."Id"
    LEFT JOIN public."vw_Stores" f ON a."DestinationHubId" = f."Id"
    LEFT JOIN public."vw_Stores" g ON a."CurrentHubId" = g."Id"
    LEFT JOIN pkg_status h ON a."PackageCode" = h."PackageCode"
WHERE
    "PackageStatus" IN ('Assigned Delivery Man', 'Pending At LM')
    AND "Rank" = 1
    AND f."Name" IN ('3PL','Probartak','Patenga Hub','Agrabad Hub','Carry Bee')
ORDER BY
    "CreatedAt" DESC