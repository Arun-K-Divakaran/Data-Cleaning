create database world_layoffs;

select *
from world_layoffs.layoffs;


-- first we want to create a staging table.
create table world_layoffs.layoffs_staging
like world_layoffs.layoffs;

select *
from world_layoffs.layoffs_staging;

insert into world_layoffs.layoffs_staging
select *
from world_layoffs.layoffs;


-- remove duplicates
with duplicate_cte as
(
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from world_layoffs.layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;

select *
from world_layoffs.layoffs_staging
where company = 'Casper';

CREATE TABLE `world_layoffs.layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from world_layoffs.layoffs_staging2;

insert into world_layoffs.layoffs_staging2
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from world_layoffs.layoffs_staging;

select *
from world_layoffs.layoffs_staging2
where row_num > 1;

delete
from world_layoffs.layoffs_staging2
where row_num > 1;

select *
from world_layoffs.layoffs_staging2;


-- standardize data
select company, trim(company)
from world_layoffs.layoffs_staging2;

update world_layoffs.layoffs_staging2
set company = trim(company);

select *
from world_layoffs.layoffs_staging2
where industry like 'Crypto%';

update world_layoffs.layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct location
from world_layoffs.layoffs_staging2;

select distinct country
from world_layoffs.layoffs_staging2
order by 1;

select *
from world_layoffs.layoffs_staging2
where country like 'United States%';

select distinct country, trim(trailing '.' from country)
from world_layoffs.layoffs_staging2
order by 1;

update world_layoffs.layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

select `date`,
str_to_date(`date`, '%m/%d/%Y')
from world_layoffs.layoffs_staging2;

update world_layoffs.layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

select `date`
from world_layoffs.layoffs_staging2;

alter table world_layoffs.layoffs_staging2
modify column `date` date;

select *
from world_layoffs.layoffs_staging2;


-- look at null values
select *
from world_layoffs.layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

update world_layoffs.layoffs_staging2
set industry = null
where industry = '';

select *
from world_layoffs.layoffs_staging2
where industry is null
or industry = '';

select *
from world_layoffs.layoffs_staging2
where company = 'Airbnb';

select t1.industry, t2.industry
from world_layoffs.layoffs_staging2 t1
join world_layoffs.layoffs_staging2 t2
on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update world_layoffs.layoffs_staging2 t1
join world_layoffs.layoffs_staging2 t2
on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

select *
from world_layoffs.layoffs_staging2;

select *
from world_layoffs.layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete
from world_layoffs.layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;


-- remove any columns & rows that are not necessary
alter table world_layoffs.layoffs_staging2
drop column row_num;

select *
from world_layoffs.layoffs_staging2;