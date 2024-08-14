CREATE DATABASE layoffs;

USE layoffs;

SELECT * 
FROM STAGING;

-- CREATE STAGING -- 
CREATE TABLE STAGING
SELECT * 
FROM layoffs;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,industry, total_laid_off,percentage_laid_off, 'date') AS row_num
FROM staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,industry, total_laid_off,percentage_laid_off, date, stage, country, funds_raised_millions) AS row_num
FROM staging
)
SELECT *
FROM duplicate_cte
WHERE row_num>1;



SELECT *
FROM staging
WHERE company = 'Wildlife Studios';

-- SECOND STAGING FOR UPDATED DATA -- 
CREATE TABLE `staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` bigint DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM staging2;

INSERT INTO staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,industry, total_laid_off,percentage_laid_off, date, stage, country, funds_raised_millions) AS row_num
FROM staging;


-- CHECKING FOR DUPLICATE ROWS
SELECT *
FROM staging2
WHERE row_num>1;

DELETE 
FROM staging2
WHERE row_num>1;

-- STANDARDIZING DATA --

SELECT company, TRIM(company)
FROM staging2;

UPDATE staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM staging2
ORDER BY 1;

SELECT *
FROM staging2
WHERE industry LIKE '%Crypto%';

UPDATE staging2
SET industry = 'Crypto'
WHERE industry LIKE '%Crypto%';

SELECT  DISTINCT COUNTRY
FROM staging2
ORDER BY 1;

SELECT DISTINCT country , TRIM(TRAILING '.' FROM country )
FROM staging2
ORDER BY 1;

UPDATE staging2
set country = TRIM(TRAILING '.' FROM country )
WHERE country LIKE 'United States%';

SELECT `date`
FROM staging2;

UPDATE staging2 
SET date = str_to_date(`date`,'%m/%d/%Y');

ALTER TABLE staging2 
MODIFY COLUMN `date` DATE;


-- NULL AND BLANK VALUES -- 

SELECT * 
FROM staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM staging2
WHERE industry IS NULL
OR industry = '' ;


SELECT *
FROM staging2
WHERE company = 'Airbnb';

SELECT st1.industry, st2.industry
FROM staging2 st1
JOIN staging2 st2
ON st1.company=st2.company
AND st1.location=st2.location
WHERE (st1.industry IS NULL OR st1.industry='')
AND st2.industry IS NOT NULL;



UPDATE staging2 
SET industry = 'Travel'
WHERE company like '%Airbnb%';

UPDATE staging2 
SET industry = NULL
WHERE industry= '';

UPDATE staging2 t1
JOIN staging t2 
ON t1.company=t2.company 
SET t1.industry=t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL; 

SELECT * 
FROM staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE  
FROM staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM staging2;

ALTER TABLE staging2
DROP row_num;



-- EXPLORATORY DATA ANALYSIS --

SELECT * 
FROM staging2;

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM staging2;

SELECT * 
FROM staging2
WHERE percentage_laid_off=1 
ORDER BY total_laid_off DESC;

SELECT * 
FROM staging2
WHERE percentage_laid_off=1 
ORDER BY funds_raised_millions DESC;

SELECT company, SUM(total_laid_off) 
FROM staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT MIN(`date`), MAX(`date`)
FROM staging2;

SELECT industry, SUM(total_laid_off) 
FROM staging2
GROUP BY industry
ORDER BY 2 DESC;

SELECT country, SUM(total_laid_off) 
FROM staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT year(`date`), SUM(total_laid_off) 
FROM staging2
GROUP BY year(`date`)
ORDER BY 2 DESC;

SELECT stage, SUM(total_laid_off) 
FROM staging2
GROUP BY stage
ORDER BY 2 DESC;

SELECT company, SUM(percentage_laid_off) 
FROM staging2
GROUP BY company
ORDER BY 2 DESC;


-- ROLLING SUM--

SELECT SUBSTRING(`date`,1,7) AS `Month`, SUM(total_laid_off)
FROM staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1;

WITH rolling_Total AS
(
SELECT SUBSTRING(`date`,1,7) AS `Month`, SUM(total_laid_off) AS total_off
FROM staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1
)
SELECT `Month`,total_off, SUM(total_off) OVER(ORDER BY `Month`) AS rolling_total
FROM rolling_Total;


-- BY COMPANY --
-- total laid off by company, year, using dense rank and common table ecpression --

WITH rc (company, Year, total_laid_off) AS 
(
SELECT company, YEAR(`date`) AS `Year`, SUM(total_laid_off) AS total_off
FROM staging2
GROUP by company ,  YEAR(`date`)
ORDER BY 1
) , company_year_ranks As
(SELECT *, DENSE_RANK() OVER (PARTITION BY Year ORDER BY total_laid_off DESC) as ranks
FROM rc
WHERE Year IS NOT NULL
ORDER BY ranks
)
SELECT * 
FROM company_year_ranks
WHERE ranks<=5;