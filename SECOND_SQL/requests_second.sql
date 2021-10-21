/* #1 */
WITH MAN_THREE_MONTH_SUM AS
         (SELECT MANAGER_ID,
                 SALE_DATE,
                 MANAGER_FIRST_NAME,
                 MANAGER_LAST_NAME,
                 SALE_AMOUNT,
                 SUM(SALE_AMOUNT) OVER (
                     PARTITION BY MANAGER_ID
                     ORDER BY SALE_DATE RANGE BETWEEN INTERVAL '3' MONTH PRECEDING AND
                     INTERVAL '1' MONTH PRECEDING
                     ) AS SUM_BY_THREE_MONTHS
          FROM V_FACT_SALE
          WHERE SALE_DATE BETWEEN TO_DATE('2013-10-01', 'YYYY-MM-DD') AND TO_DATE('2014-12-31', 'YYYY-MM-DD')
         )
SELECT
    DISTINCT MANAGER_ID,
             MANAGER_FIRST_NAME,
             MANAGER_LAST_NAME,
             TO_CHAR(SALE_DATE, 'MM') AS MAN_MONTH,
             (SUM_BY_THREE_MONTHS*0.05) AS BONUS,
             SUM_BY_THREE_MONTHS
FROM MAN_THREE_MONTH_SUM
WHERE SUM_BY_THREE_MONTHS IN (
    SELECT MAX(SUM_BY_THREE_MONTHS) OVER (
        PARTITION BY TO_CHAR(SALE_DATE, 'MM')
        ) AS MAX_BY_MONTH
    FROM MAN_THREE_MONTH_SUM
    WHERE SALE_DATE BETWEEN TO_DATE('2014-01-01', 'YYYY-MM-DD') AND TO_DATE('2014-12-31', 'YYYY-MM-DD')
)
ORDER BY MAN_MONTH;

/* #2 */
WITH ALL_SUMS_BY_OFFICE AS (
    SELECT OFFICE_ID,
           TO_CHAR(SALE_DATE, 'YYYY') AS YEAR_OFFICE,
           SUM(SALE_AMOUNT) OVER (
               PARTITION BY OFFICE_ID
               ORDER BY SALE_DATE RANGE BETWEEN INTERVAL '1' YEAR PRECEDING AND
               CURRENT ROW
               ) AS SUM_BY_OFFICE
    FROM V_FACT_SALE
    WHERE SALE_DATE BETWEEN TO_DATE('2013-01-01', 'YYYY-MM-DD') AND TO_DATE('2014-12-31', 'YYYY-MM-DD')
),
     SUMS_BY_OFFICE_BY_YEAR AS (
         SELECT DISTINCT OFFICE_ID,
                         YEAR_OFFICE,
                         MAX(SUM_BY_OFFICE) OVER (
                             PARTITION BY OFFICE_ID, YEAR_OFFICE
                             ORDER BY YEAR_OFFICE ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                             ) AS SUM_BY_YEAR_OFFICE
         FROM ALL_SUMS_BY_OFFICE
     )
SELECT DISTINCT SBOBY.OFFICE_ID,
                CITY_NAME,
                COUNTRY,
                YEAR_OFFICE,
                --SUM_BY_YEAR_OFFICE / SUM(SUM_BY_YEAR_OFFICE) OVER(
                --    PARTITION BY YEAR_OFFICE
                --    ORDER BY YEAR_OFFICE ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                --    ),
                RATIO_TO_REPORT(SUM_BY_YEAR_OFFICE) OVER (
                    PARTITION BY YEAR_OFFICE
                    ) AS PROPORTION
FROM SUMS_BY_OFFICE_BY_YEAR SBOBY
         INNER JOIN V_FACT_SALE ON V_FACT_SALE.OFFICE_ID = SBOBY.OFFICE_ID
ORDER BY OFFICE_ID;

/* #3 */
WITH PR_SUM_BY_MONTH AS (
    SELECT DISTINCT PRODUCT_ID,
                    TO_CHAR(SALE_DATE, 'MM') AS PR_MONTH,
                    SUM(SALE_QTY) OVER (
                        PARTITION BY PRODUCT_ID
                        ORDER BY TO_CHAR(SALE_DATE, 'MM') RANGE BETWEEN CURRENT ROW
                        AND CURRENT ROW
                        )                    AS ALL_SUMS_BY_MONTH,
                    SUM(SALE_QTY) OVER (
                        PARTITION BY PRODUCT_ID
                        ORDER BY TO_NUMBER(TO_CHAR(SALE_DATE, 'MM')) RANGE BETWEEN 1 PRECEDING
                        AND CURRENT ROW
                        )                    AS ALL_SUMS_BY_TWO_MONTHS/*,
           SUM(SALE_QTY) OVER (
               PARTITION BY PRODUCT_ID
               ORDER BY SALE_DATE RANGE BETWEEN INTERVAL '1' month PRECEDING
               AND CURRENT ROW
               )                    AS ALL_SUMS_BY_TWO_MONTHS_2*/
    FROM V_FACT_SALE
    WHERE SALE_DATE BETWEEN TO_DATE('2014-01-01', 'YYYY-MM-DD') AND TO_DATE('2014-06-30', 'YYYY-MM-DD')
),
     PR_SUM_PROPERTIES AS (
         SELECT DISTINCT PRODUCT_ID,
                         PR_MONTH,
                         MAX(ALL_SUMS_BY_MONTH) OVER (
                             PARTITION BY PRODUCT_ID, PR_MONTH
                             ORDER BY PR_MONTH ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                             ) AS SUM_BY_MONTH,
                         MAX(ALL_SUMS_BY_TWO_MONTHS) OVER (
                             PARTITION BY PRODUCT_ID, PR_MONTH
                             ORDER BY PR_MONTH ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                             ) AS SUM_BY_TWO_MONTHS
         FROM PR_SUM_BY_MONTH
     )
SELECT DISTINCT PRODUCT_ID,
                PR_MONTH,
                SUM_BY_MONTH / LAG(SUM_BY_MONTH, 1, SUM_BY_MONTH) OVER (
                    PARTITION BY PRODUCT_ID
                    ORDER BY PR_MONTH
                    ) AS RELATION,
                DECODE(SUM_BY_MONTH - SUM_BY_TWO_MONTHS, 0,
                       SUM_BY_MONTH / SUM_BY_MONTH,
                       SUM_BY_MONTH / (SUM_BY_TWO_MONTHS - SUM_BY_MONTH)) AS RELATION_WITHOUT_LAG
FROM PR_SUM_PROPERTIES
ORDER BY PRODUCT_ID;

/* #4 */
WITH COMP_SUM_BY_MONTH AS (
    SELECT DISTINCT TO_CHAR(SALE_DATE, 'MM') AS COMP_MONTH,
                    SUM(SALE_AMOUNT) OVER (
                        ORDER BY TO_CHAR(SALE_DATE, 'MM') RANGE BETWEEN CURRENT ROW
                        AND CURRENT ROW
                        )                    AS SUM_COMP_BY_MONTH,
                    SUM(SALE_AMOUNT) OVER (
                        ORDER BY TO_NUMBER(TO_CHAR(SALE_DATE, 'MM')) RANGE BETWEEN 3 PRECEDING
                        AND CURRENT ROW
                        )                    AS THREE_MONTH,
                    SUM(SALE_AMOUNT) OVER (
                        ORDER BY TO_NUMBER(TO_CHAR(SALE_DATE, 'MM')) RANGE BETWEEN 12 PRECEDING
                        AND CURRENT ROW
                        )                    AS TWELVE_MONTH
    FROM V_FACT_SALE
    WHERE SALE_DATE BETWEEN TO_DATE('2014-01-01', 'YYYY-MM-DD') AND TO_DATE('2014-12-31', 'YYYY-MM-DD')
)
    (SELECT COMP_MONTH,
            SUM_COMP_BY_MONTH,
            SUM(SUM_COMP_BY_MONTH) OVER (
                ORDER BY COMP_MONTH RANGE BETWEEN UNBOUNDED PRECEDING
                AND CURRENT ROW
                ) AS FULL_SUM_TO_CURR_MONTH,
            'MONTHLY'
     FROM COMP_SUM_BY_MONTH
    )
UNION ALL
(SELECT *
 FROM (SELECT COMP_MONTH, THREE_MONTH, TWELVE_MONTH, 'QUARTERLY'
       FROM COMP_SUM_BY_MONTH
       WHERE COMP_MONTH = 3
      )
 UNION
 (SELECT COMP_MONTH, THREE_MONTH, TWELVE_MONTH, 'QUARTERLY'
  FROM COMP_SUM_BY_MONTH
  WHERE COMP_MONTH = 6
 )
 UNION
 (SELECT COMP_MONTH, THREE_MONTH, TWELVE_MONTH, 'QUARTERLY'
  FROM COMP_SUM_BY_MONTH
  WHERE COMP_SUM_BY_MONTH.COMP_MONTH = 9)
 UNION
 (SELECT COMP_MONTH, THREE_MONTH, TWELVE_MONTH, 'QUARTERLY'
  FROM COMP_SUM_BY_MONTH
  WHERE COMP_SUM_BY_MONTH.COMP_MONTH = 12)
);

/* #5 */
SELECT PRODUCT_ID,
       PRODUCT_NAME
FROM V_FACT_SALE;
WITH PROD_COSTS AS (
    SELECT DISTINCT PRODUCT_ID,
                    PRODUCT_NAME,
                    AVG(SALE_AMOUNT) OVER (
                        PARTITION BY PRODUCT_ID
                        ) AS PROD_COST_MEAN,
                    SUM(SALE_AMOUNT) OVER (
                        PARTITION BY PRODUCT_ID
                        ) AS SUM_BY_PROD,
                    SUM(SALE_AMOUNT) OVER ( ) AS TOTAL_SALE_AMOUNT
    FROM V_FACT_SALE
    WHERE SALE_DATE BETWEEN TO_DATE('2014-01-01', 'YYYY-MM-DD') AND TO_DATE('2014-12-31', 'YYYY-MM-DD')
),
     PROD_COSTS_RELATION AS (
         SELECT DISTINCT PRODUCT_ID,
                         DENSE_RANK() OVER (
                             ORDER BY PROD_COST_MEAN
                             )                          AS PROD_RANK,
                         COUNT(PROD_COST_MEAN) OVER ( ) AS RANK_AMOUNT
         FROM PROD_COSTS
     )
SELECT PCR.PRODUCT_ID,
       PRODUCT_NAME,
       PROD_RANK,
    /*RATIO_TO_REPORT(SUM_BY_PROD) OVER ( ), */
       SUM_BY_PROD / TOTAL_SALE_AMOUNT AS PERCENT,
       TOTAL_SALE_AMOUNT
FROM PROD_COSTS_RELATION PCR
         INNER JOIN PROD_COSTS ON PROD_COSTS.PRODUCT_ID = PCR.PRODUCT_ID
WHERE (PROD_RANK < CEIL(RANK_AMOUNT * 0.1)) OR (PROD_RANK >= CEIL(RANK_AMOUNT * 0.9))
ORDER BY PROD_RANK;

/* #6 */
WITH SUMS_BY_MANAGER AS (
    SELECT DISTINCT COUNTRY,
                    MANAGER_ID,
                    MANAGER_FIRST_NAME,
                    MANAGER_LAST_NAME,
                    SUM(SALE_AMOUNT) OVER (
                        PARTITION BY MANAGER_ID
                        ) AS SUM_BY_MAN
    FROM V_FACT_SALE
    WHERE MANAGER_ID IS NOT NULL AND
        SALE_DATE BETWEEN TO_DATE('2014-01-01', 'YYYY-MM-DD') AND TO_DATE('2014-12-31', 'YYYY-MM-DD')
),
     MAN_RANK_BY_COUNTRY AS (
         SELECT COUNTRY,
                MANAGER_ID,
                MANAGER_FIRST_NAME,
                MANAGER_LAST_NAME,
                DENSE_RANK() OVER (
                    PARTITION BY COUNTRY
                    ORDER BY SUM_BY_MAN
                    ) AS RANK_BY_COUNTRY,
                CONCAT(CONCAT(MANAGER_LAST_NAME, ' '), MANAGER_FIRST_NAME) AS LAST_FIRST_NAME
         FROM SUMS_BY_MANAGER
     )
SELECT DISTINCT COUNTRY,
                --CONCAT(CONCAT(MANAGER_LAST_NAME, ' '), MANAGER_FIRST_NAME),
                LISTAGG(LAST_FIRST_NAME, ', ') WITHIN GROUP (ORDER BY RANK_BY_COUNTRY) OVER (PARTITION BY COUNTRY)
                --RANK_BY_COUNTRY,
                --MANAGER_ID
FROM MAN_RANK_BY_COUNTRY
WHERE RANK_BY_COUNTRY IN (1, 2 ,3);

/* #7 */
WITH MIN_MAX_COST AS (
    SELECT DISTINCT PRODUCT_ID,
                    TO_CHAR(SALE_DATE, 'MM') AS PROD_MONTH,
                    MIN(SALE_PRICE) OVER (
                        PARTITION BY TO_CHAR(SALE_DATE, 'MM')
                        )                    AS PROD_COST_MIN,
                    MAX(SALE_PRICE) OVER (
                        PARTITION BY TO_CHAR(SALE_DATE, 'MM')
                        )                    AS PROD_COST_MAX
    FROM V_FACT_SALE
    WHERE SALE_DATE BETWEEN TO_DATE('2014-01-01', 'YYYY-MM-DD') AND TO_DATE('2014-12-31', 'YYYY-MM-DD')
)
SELECT PRODUCT_ID,
       SALE_PRICE,
       SALE_DATE
FROM V_FACT_SALE
WHERE SALE_DATE BETWEEN TO_DATE('2014-01-01', 'YYYY-MM-DD') AND TO_DATE('2014-12-31', 'YYYY-MM-DD')



