# Notes
KPI Cards
These four cards give you a national snapshot for the selected year. Solar and wind show total installed capacity across all 49 states. Renewable share and CO2 are averages across states. Use the year filter at the top to update everything at once.

GDP per Capita vs Composite Alignment
This chart tracks how well state wealth predicts renewable investment year by year. A higher line means wealthier states are investing more in line with their climate potential. The drop after 2017 is the key finding here as it tells us money stopped being the main driver right around when renewable costs started falling sharply.

Country Alignment Rankings
This bar chart ranks each country by how well their renewable investment matches their climate resources. Germany leads every year not because it has the best sun or wind but because its policy environment drives investment far beyond what the climate alone would predict. Mexico sits at the bottom despite having excellent solar conditions.

International Composite Alignment Over Time
This line chart shows how each country has trended from 2005 to 2020. The lines start to diverge around 2010 which is when European feed in tariff policies really took hold. The US sits in the middle of the pack improving gradually but never catching the European leaders.

Solar vs Wind Alignment Pearson r
This is one of the most important charts in the dashboard. It shows that wind has always been the stronger signal. States with good wind tend to invest in wind. The solar line starts low and climbs over time which reflects how cheap solar panels became after about 2015 making it more attractive even in states that are not the sunniest.

Composite Alignment by State Top 10 and Bottom 10
New Mexico Colorado and Texas lead. Kentucky Louisiana and Alabama sit at the bottom. The geographic split is stark and consistent across years. The Southeast is persistently underinvesting relative to its climate potential.

Wind Investment vs Wind Resource
Each dot is a state. The further right a state is the windier it is. The higher up it is the more wind capacity it has installed per person. States above the trend line are overperforming relative to their wind conditions. Iowa and North Dakota stand out here.

Solar Investment vs Solar Resource
Same idea as the wind scatter but for solar. The relationship is messier than wind which lines up with the lower Pearson r values we see in the trend chart. Nevada leads in per capita solar installation. State incentive programs explain a lot of the variation that climate alone cannot.

Composite Alignment Map
Green means a state is investing in line with or above its climate potential. Red means it is underinvesting. The pattern is almost exactly what you would expect from looking at US energy policy history. The Mountain West and New England are green. The Southeast is red.

Alignment Score Distribution
This histogram shows how alignment scores are spread across all states. The peak sits just below 0.5 meaning most states are in the middle. The rightward shift over time is encouraging and shows the national trend is moving in the right direction.

Alignment Tier and Dominant Energy Source Sankey
This diagram shows which energy source dominates in each alignment tier. The finding that even high aligned states often lead in coal is the counterintuitive result here. Wyoming is the best example of a state that scores well on alignment while still producing enormous amounts of coal.

Energy Mix by State
All values are normalized per million residents so large states and small states can be compared fairly. Nevada leads in solar per person and North Dakota leads in wind per person. Wyoming leads in coal per person which explains its complicated alignment story.

Coal to Renewable Shift 2008 vs 2023
This chart shows which states have changed the most over the study period. The states at the left side of the chart have barely moved. The states at the right have transformed dramatically. Iowa and South Dakota went from almost no renewables to leading the country.

CO2 vs Renewable Share
Higher renewable share tends to mean slightly lower CO2 per capita but the relationship is weaker than you might expect. This is partly because states like Wyoming burn coal for export not just local use and partly because some high renewable states like Washington rely heavily on hydro which is clean but was already in place before our study period.

Electricity Price vs Renewable Share
Renewable heavy states tend to pay slightly less per kilowatt hour on average. The relationship is not strong but the direction is consistent with what we know about falling wind and solar costs.

State Energy Data Table
Every metric for every state in one place. Sortable by any column. Useful for finding specific states or outliers that the charts might not highlight.

State Energy Signature Radar
The dashed gray polygon is the national average. Any state polygon that extends beyond the gray line on a given axis is outperforming the national mean on that metric. It is a quick way to see whether a state is well rounded or specialized in one area.

Animated Investment vs Resource Scatter
Toggle between solar and wind and between static and animated. The animated view plays through each year so you can watch the relationship between climate resources and investment evolve from 2008 to 2023.

Composite Alignment Over Time
Select any combination of states from the dropdown to compare their trajectories. The flat lines near zero before 2015 for states like Alabama reflect that solar and wind capacity was essentially zero before that point. The steep climbs after 2015 in states like Texas and New Mexico reflect the utility scale buildout that followed the collapse of solar panel prices.

Renewable Share Top 5 vs Selected State
This panel puts the selected state in context against the national leaders. South Dakota Iowa and Maine consistently lead. Use this alongside the alignment chart to understand whether a states renewable share is actually proportional to its climate resources or just a reflection of abundant hydro or policy incentives.

Data Sources

Data sources: U.S. Energy Information Administration for energy capacity generation and consumption. National Renewable Energy Laboratory and National Solar Radiation Database for state level solar radiation. National Oceanic and Atmospheric Administration for climate observations. U.S. Bureau of Economic Analysis and U.S. Census Bureau for GDP and population. Our World in Data for international energy statistics. Photovoltaic Geographical Information System for international solar radiation data. All data is aggregated to the state year or country year level covering 2008 to 2023 for U.S. state analysis and 2005 to 2020 for international comparisons. Alaska is excluded from all U.S. state analysis due to NSRDB satellite coverage limitations and grid incomparability with the remaining 49 states. District of Columbia and Puerto Rico are excluded as non state geographies. International data coverage ends at 2020 due to PVGIS availability constraints for North American countries.

Project Notes

The composite alignment score is the core analytical contribution of this project. It is calculated as a percentile ranked composite of solar and wind investment relative to climate potential averaged together to produce a single 0 to 1 score per state per year
Alaska stores NULLs rather than zeros in the database to preserve data honesty rather than implying zero investment
The GDP Pearson r panel is the most academically interesting single chart in the dashboard because the post 2017 decline is a quantitative finding that directly supports the written research conclusions

Wind aligns more strongly than solar in every year of the study period which was the headline finding confirmed by the climate alignment correlation view

The international table uses PVGIS for solar radiation backfilled with country averages for 2016 to 2020 where data was unavailable
Wyoming is the most counterintuitive state in the dataset as it scores well on alignment while being the largest coal producing state in the country

The scatter plots for wind and solar both show trendlines computed using numpy polyfit to make the positive correlation immediately visible without requiring the viewer to read the Pearson r value
All monetary and energy values are population normalized using Census population data to allow fair comparison between large and small states

The dashboard title Beyond the Grid was chosen to reflect that the findings go beyond simple grid capacity numbers to ask whether investment is actually going where the climate resources are
The forecasting panel uses second degree polynomial extrapolation rather than a complex model because with only 16 data points per state simpler models are more defensible for a 20 year forecast horizon
The Sankey diagram finding that both High and Low alignment states often lead in coal reflects the complexity of the US energy system where coal dependency and renewable investment can coexist in the same state


The Composite Alignment Score

What It Is
The composite alignment score is the central analytical contribution of this project. It answers one question: is a state investing in renewables where the climate actually supports it? The score runs from 0 to 1 where 1 means a state is investing more proportionally than almost every other state relative to its climate potential and 0 means it is investing less than almost every other state.

How It Is Calculated
Step 1 Normalize investment by population and climate resource
Each state's solar and wind capacity is divided by population first to put large and small states on equal footing. That per capita number is then divided by the climate resource to ask how much a state is investing relative to what nature gave it.

Solar ratio = solar MW per million people divided by average GHI
Wind ratio = wind MW per million people divided by average wind speed

Step 2 Percentile rank each ratio within the year
Each ratio is ranked against all other states in the same year using PERCENT_RANK in SQL. This removes the national growth trend so that a state is not penalized for being in 2008 when almost nobody had solar, or rewarded just for being in 2023 when solar was everywhere. A score of 0.75 means a state invests more proportionally than 75 percent of other states that year.

Solar Score = percentile rank of the solar ratio among all states that year
Wind Score = percentile rank of the wind ratio among all states that year

Step 3 Average the two scores

Composite Alignment = (Solar Score + Wind Score) / 2


Why Percentile Rank Instead of a Raw Number
Raw investment numbers grow every year as renewable energy expands nationally. A state that installed 500 MW of wind in 2010 looks very different from a state that installed 500 MW in 2023 because the national context changed dramatically. Percentile ranking within each year removes that growth trend and asks only where each state stands relative to its peers that year making comparisons across time meaningful.

Tables and Views Built to Support This
Staging tables were the first layer. Each data source landed in its own raw table preserving the original structure before any transformation. EIA data went into eight staging tables covering generation, capacity, emissions, sales, coal, natural gas, and total energy. NOAA climate data landed in one table with over 160,000 station level rows. NSRDB solar data, PVGIS international solar, OWID international energy, BEA economic data, and Census population each got their own staging table.
Production tables were the second layer. Six core tables were built from the staging data.

The state table holds the 50 state reference records with FIPS codes, abbreviations, and lat/lon coordinates. Puerto Rico and DC were removed as non state geographies and Alaska was retained but stores NULLs rather than zeros for NSRDB data to preserve data honesty.
The year table holds the full time dimension from 2001 to 2023 with a decade grouping column.
The climate table aggregates NOAA station observations and NSRDB readings into one row per state per year with average temperature, wind speed, sunshine, GHI, and DNI. Alaska has NULLs for solar radiation fields.
The energy table consolidates eight EIA staging tables into one wide row per state per year covering solar MW, wind MW, generation for both, CO2 emissions, retail sales, average price, renewable share percentage, and coal consumption. A column rename was required to fix a hyphen in the original EIA field name before the insert could run.
The economic table joins BEA GDP data with Census population data using a two digit FIPS key to bridge the mismatch between the five digit BEA FIPS and the two digit Census FIPS. GDP per capita is computed as a derived column.
The international table joins OWID country level energy statistics with PVGIS solar radiation data scoped to 2005 to 2020 where both sources overlap. Missing GHI values for the US, Canada, and Mexico between 2016 and 2020 were backfilled using each country's own historical average or the NSRDB national average for the US.

Analytical tables and views were the third layer.

The composite alignment table is the central output holding one score per state per year alongside GDP per capita. It is built using two CTEs: per capita to compute the normalized investment ratios, and ranked to apply PERCENT_RANK within each year before averaging.
The gdp alignment correlation view computes the annual Pearson r between GDP per capita and composite alignment and recalculates automatically if the underlying table is updated.
The climate alignment correlation view computes the annual Pearson r between each climate resource and its corresponding per capita investment, separately for solar and wind.
The state energy profile view joins composite alignment with energy and climate data for a complete per state per year picture used by the scatter plots and table panel.
The international alignment view applies the same PERCENT_RANK logic as the US state model to rank each country by solar and wind investment relative to GHI each year.
The coal to renewable shift view compares each state's 2008 and 2023 renewable share side by side to show how much each state has transformed over the study period.
The gdp alignment rank view shows each state's GDP rank and alignment rank side by side for 2023 to reveal which wealthy states underperform and which lower income states overperform relative to their economic size.


Queries Powering the Dashboard
All dashboard panels pull from these analytical layers. The KPI cards average solar MW, wind MW, renewable share, and CO2 across all states for the selected year. The radar chart pulls six population normalized metrics per state and normalizes them again in Python using min max scaling before plotting. The animated scatter pulls all years of per capita solar and wind investment alongside the corresponding climate resource so the relationship can be animated frame by frame. The histogram pulls the raw composite alignment score for every state and year with no joins needed. The Sankey pulls state level alignment and energy mix data and computes alignment tiers and dominant energy source in Python before building the node and link structure. The exploratory panels use the full profile query joining composite alignment, energy, and economic data filtered to the selected year.

### last minute task

- fix graphs when changing light and dark mode
- check the foot notes to more generalize as filter of year was added
- submit to canves
  


### POTENTIAL TASK WE CAN DO 
Forecasting Panel: Data Sources
From the composite_alignment table

state: to filter by selected state
year: as the time axis from 2008 to 2023
composite_alignment: as one of the two forecast targets

From the energy table joined to composite_alignment

renewable_shar; e_pct: as the second forecast target
state and yearas join keys

National aggregate query

Averages composite_alignment across all 49 states by year from the composite_alignment table
Averages renewable_share_pct across all 49 states by year from the energy table
Groups by year only so the result is one national row per year

No new tables needed

Everything required already exists in the composite_alignment and energy tables
No joins to climate, economic, or state tables are needed since the forecast is purely univariate it only needs the historical values of the target variable over time
Population normalization and climate resource adjustments are already baked into the composite alignment score so no recalculation is needed at the forecasting stage

Limitations

Assumes historical trends continue without major policy shifts or technology breakthroughs
Cannot account for regulatory changes, grid infrastructure investments, or economic shocks
Should be interpreted as trend extrapolation not prediction






