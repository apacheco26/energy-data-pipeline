# energy-data-pipeline
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


C-mposite Score 

The Equation
Step 1 Normalize investment by population and climate resource

Solar ratio = solar MW per million people divided by average GHI
Wind ratio = wind MW per million people divided by average wind speed

Step 2 Percentile rank each ratio within the year

Solar Score = where a state's solar ratio ranks compared to all other states that same year (0 to 1)
Wind Score = where a state's wind ratio ranks compared to all other states that same year (0 to 1)

Step 3 Average the two scores

Composite Alignment = (Solar Score + Wind Score) / 2


Why Percentile Rank Instead of a Raw Number
Raw investment numbers grow every year as renewable energy expands nationally. A state that installed 500 MW of wind in 2010 looks very different from a state that installed 500 MW in 2023 because the national context changed dramatically. Percentile ranking within each year removes that national growth trend and asks only where each state stands relative to its peers that year making comparisons across time meaningful.


Tables Built to Support This

Staging tables were the first layer. Each data source landed in its own raw table preserving the original structure before any transformation. EIA data went into eight staging tables covering generation, capacity, emissions, sales, coal, natural gas, and total energy. NOAA climate data landed in one table with over 160,000 station level rows. NSRDB solar data, PVGIS international solar, OWID 
international energy, BEA economic data, and Census population each got their own staging table.

Production tables were the second layer. Six core tables were built from the staging data. The state table holds the 49 state reference records with FIPS codes, abbreviations, and lat/lon coordinates. The year table holds the 2008 to 2023 time dimension. The climate table aggregates NOAA station observations and NSRDB readings up to one row per state per year with average temperature, wind speed, GHI, and DNI. The energy table consolidates all EIA sources into one wide row per state per year with solar MW, wind MW, generation, emissions, retail sales, price, renewable share, and coal consumption. The economic table joins BEA and Census data to produce GDP per capita and population per state per year. The international table holds country year observations for 12 countries with renewable share, solar share, wind share, GDP per capita, and average GHI.

Analytical tables and views were the third layer built on top of production. The composite alignment table is the central analytical output holding one alignment score per state per year alongside GDP per capita. The gdp alignment correlation view computes the Pearson r between GDP and alignment for each year. The climate alignment correlation view computes the Pearson r between each climate resource and its corresponding investment for each year. The state energy profile view joins composite alignment with energy and climate data for a complete picture per state per year. The international alignment view computes percentile ranked solar and wind alignment scores per country per year. The coal to renewable shift view compares each states 2008 and 2023 renewable share side by side. The composite alignment grafana view formats the data with timestamps for time series visualization.

