SELECT *
From data_layoffs;

# So what do we do in data cleaning of data.

# 1: Remove Duplicates
# 2: Standerdize the data
# 3: Remove any null or blank values
# 4: Removing any columns which are created in the process or not required.


CREATE TABLE layoffs_staging
LIKE data_layoffs ;

SELECT *
From layoffs_staging;

INSERT layoffs_staging
SELECT * 
FROM data_layoffs ;

# Removing duplicates

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, stage, country, funds_raised_millions, total_laid_off, percentage_laid_off, `date`) as row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, stage, country, funds_raised_millions, total_laid_off, percentage_laid_off, `date`) as row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
From layoffs_staging
WHERE company= "Casper";

# We can remove these duplicates using an delete statement with the cte we have created just now.

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, stage, country, funds_raised_millions, total_laid_off, percentage_laid_off, `date`) as row_num
FROM layoffs_staging
)
DELETE 
FROM duplicate_cte
WHERE row_num > 1;
# This generally works in microsoft tool, but in sql we need to do things a little bit different.
# we will create a new stage table and then we remove those duplicates by adding a new coloumn to the newly created stage table
# from the layoffs_staging on the left schemas, use copy to clipboard and create statement.

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
From layoffs_staging2 ;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, stage, country, funds_raised_millions, total_laid_off, percentage_laid_off, `date`) as row_num
FROM layoffs_staging ;

SELECT *
From layoffs_staging2 ;
# Now as the whole data is into a new table, we can remove the duplicates directly.
SELECT *
From layoffs_staging2 
WHERE row_num > 1;

DELETE
From layoffs_staging2 
WHERE row_num > 1;

SELECT *
From layoffs_staging2 
WHERE row_num > 1;

# we can see all the duplicates are removed from the table with this, use this format only when we dont have indexing,
# If we have indexing, use indexing directly to remove the duplicates.

# Now comes Standerdizing.
SELECT *
From layoffs_staging2 ;
# We can see that we have spaces, additional characters etc in the company name, so lets remove them.

SELECT company, TRIM(company)
From layoffs_staging2 ; # We can see the spaces before the E INC and Included Health are now removed.

# Lets update this for the table 
UPDATE layoffs_staging2
SET company = TRIM(company) ;

#Now lets check for the industry column

SELECT distinct industry
FROM layoffs_staging2
order by 1 ; 

# We need to remove those empty columns, we will do it once later, but if we notice, columns crypto, cryptocurrency, crypto currency, all are same, this will result in colinearity later in analysis of data, hence we need to merge them.
# In regular work if u have more such columns u think speaking the same thing, merge them all.

SELECT *
from layoffs_staging2
where industry like 'Crypto%'
;
update layoffs_staging2
set industry ='Crypto' 
where industry like 'Crypto%';

SELECT distinct industry
from layoffs_staging2 ;
# We can see it worked perfectly.

# Now lets check for other columns and see if we need any adjustments.

SELECT *
FROM layoffs_staging2 ;

SELECT DISTINCT location
FROM layoffs_staging2 ; # looks good

SELECT DISTINCT country
FROM layoffs_staging2 
Order by 1 ; # we can see the Unitedstates needs an update, so lets fix it

SELECT *
from layoffs_staging2
where country like 'United States%';
Update layoffs_staging2
set country = 'United States'
where country like 'United States%' ;

# or if u are not sure if they are being over written, just follow the below

SELECT DISTINCT country, TRIM( TRAILING '.' FROM country)
FROM layoffs_staging2 
Order by 1 ;
Update layoffs_staging2
set country = TRIM( TRAILING '.' FROM country)
where country like 'United States%' ; # that looks neat.

# We observed that `date` was of datatype text, we must always try to keep it as datetime, So lets do that.
SELECT `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2 ;
UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

SELECT `date`
from layoffs_staging2 ; #Here the format is converted to the format of mysql, still if u select date column and see it will be text as we didnot yet modify the column, so lets do that,

# Remember to alter any table if its a copied one, never alter or drop any table if its the original one.

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE ; # Now the datatype of date is date.

#The next step obviously will be removing the null values in the data, lets do that now.

SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL ;

# We can see that there are a lot of null values and if we observe closely, we can see that for some rows both total laid offs and percentage laid off have null values, hence we can say that these are not usefull for us.
SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL ;

#Its optimal to always convert ur empty folds to nulls so when we update the values, it wont cause any issue for us.
UPDATE layoffs_staging2
SET industry = null
WHERE industry = '' ;

# We also observed that industry columm also had null and also empty values, lets find them.
SELECT * FROM layoffs_staging2
Where industry IS NULL 
OR industry = '' ;

SELECT * 
FROM layoffs_staging2
Where company = "Airbnb" ;

SELECT * FROM layoffs_staging2
Where company = "Bally's Interactive" ;

#Sometimes we will have values where few of them have null or empty values and some of them will have values updated for example
# If the company Airbnb has one industry value as empty and one as Travel we can update the empty value with travel.

SELECT * FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	on t1.company = t2.company
	# and t1.location = t2.location # only when we dont want to replace different locations for same company
where (t1.industry is null OR t1.industry = '')
and t2.industry is not null ; # This looks perfect, lets update the table now

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	on t1.company = t2.company
SET t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null ; # This is how we modify and update the values.

SELECT * FROM layoffs_staging2
WHERE industry IS NULL 
AND industry = '' ;

# Now lets complete what we observed on the top and left for the end
SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL ;

# ONLY Delete when u are 100% sure that those rows are not usefull at all, if not do not delete any rows in any project.

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL ; # All those values are now deleted.

SELECT *
FROM layoffs_staging2 ;

# Dropping the column row_num as we dont need it anymore

ALTER TABLE layoffs_staging2
DROP COLUMN row_num ;

#That looks very good dataset and we can proceed for data analysis.









