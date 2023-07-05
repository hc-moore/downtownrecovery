# Databricks notebook source
!pip install geopandas
!pip install pandas
!pip install numpy
!pip install warnings
import geopandas as gpd
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore') # ignore the slice warning/some geometry warnings that are ultimately inconsequential

# COMMAND ----------

def create_and_save_as_table(df, table_name):
    spark_df = spark.createDataFrame(df)
    spark_df.write.format('csv').saveAsTable(table_name)
    
def get_table_as_pandas_df(table_name):
    return sqlContext.sql("select * from " + table_name + " tables").toPandas()

# COMMAND ----------

sink_city_zipcode_counts = get_table_as_pandas_df("sink_city_zipcode_counts")
city_index = get_table_as_pandas_df("city_index_with_dwtn_csv")

# COMMAND ----------



# COMMAND ----------

zipcode_counts = {}
for city in sink_city_zipcode_counts["city"].unique():
    zipcode_counts[city] = sink_city_zipcode_counts[sink_city_zipcode_counts["city"]==city]

# COMMAND ----------

zipcode_counts[dbutils.widgets.get("City")]

# COMMAND ----------

city_index

# COMMAND ----------

import seaborn as sns
import matplotlib
import matplotlib.cm as cm
import matplotlib.pyplot as plt
from mpl_toolkits.axes_grid1.inset_locator import inset_axes
from scipy import stats
results = get_table_as_pandas_df('export_csv')
results["week"] = results["week"].str[3:]
sink_cities = pd.Series(results["city"].unique())
sink_cities = sink_cities[sink_cities.isin(zipcode_counts.keys())].to_numpy()

# create notebook widget to select city
dbutils.widgets.dropdown(name = 'City', defaultValue = sink_cities[0], choices = sink_cities)
# ignore these, no longer necessary but keeping in case they might be interesting/useful later
#dbutils.widgets.dropdown(name = 'Time Period', defaultValue = "precovid_vc_zipcode_count", choices = ['precovid_vc_zipcode_count', 'covid_vc_zipcode_count'])
# after selecting the week, the date dropdown will update with problem weeks for that city
#dbutils.widgets.dropdown(name = 'Date', defaultValue = sink_weeks[0], choices = sink_weeks)

# COMMAND ----------

# takes ~ 1 min to run
#Import Downtown Zipcodes - all US
us_city_index = get_table_as_pandas_df('city_index_0119_csv')
us_downtowns = get_table_as_pandas_df('us_downtowns_csv')
us_city_index["downtown_zipcodes"] = us_downtowns["_c2"].apply(lambda x: x[1:-1].split(','))
# just the sinks
sink_downtowns = us_downtowns[us_downtowns["_c0"].isin(sink_cities)]
sink_index = us_city_index[us_city_index["0"].isin(sink_cities)]
downtown_zipcodes = []
for zipcode_list in sink_index.set_index("0")["downtown_zipcodes"].values:
    for zipcode in zipcode_list:
        downtown_zipcodes.append(str.strip(zipcode))
        
# import zcta shapefile
# 'incorrect' geometry warning is not that big of a deal at the moment since this is mostly used to visualize zip codes, not project them down to the .00001th degree
zcta_shp = get_table_as_pandas_df('zcta_geo_1_csv')
zcta_shp['geometry'] = zcta_shp['geometry'].apply(shapely.wkt.loads)
zcta_shp = gpd.GeoDataFrame(zcta_shp, geometry = 'geometry')

# COMMAND ----------

# can take 1 min or so to run for especially sink-prone cities/cities with larger geographies (charlotte and dallas are the slowest, the rest don't take too long)
# plots selected type of count by zipcode for a selected city on a selected date
# for the sake of making several maps of a city squashed into a notebook's margins readable, only the downtown zipcodes are plotted as they are the thing being observed and their LQ is what matters, not the entire city's.
# each city is a little different in zip code quantity as well as shape, so some might look squashed or 'ugly'; the current setting splits the difference and made as many cities look somewhat decent as possible.
# charlotte is particularly difficult though, it has several weeks of NaN data and has a lot of area, so the maps are VERY squashed. it might not be worth visualizing with maps in a notebook or should be split up by year?

def cityplot(city_name):
    city_df = sink_city_zipcode_counts[sink_city_zipcode_counts["city"] == city_name]
    sink_weeks = results.loc[results["city"] == city_name, "week"].unique()
    city_df = city_df.loc[city_df["week"].isin(sink_weeks), :]
    city_df["week"] = pd.to_datetime(city_df["week"])
    city_df = city_df.groupby(["week", "zipcode"]).sum().reset_index().melt(id_vars = ["week", "zipcode"], var_name = "Time Period", value_name = "count")
    city_df = city_df[city_df["zipcode"].isin(downtown_zipcodes)]
    city_df.loc[city_df["count"] == 0, "count"] = np.nan 
    
    low_end = np.nanmin(city_df["count"])
    high_end = np.nanmax(city_df["count"])
    
    city_df_sf = zcta_shp.merge(city_df, left_on = "ZCTA5CE10", right_on = "zipcode", how = "inner")
    city_df_sf.crs = "EPSG:4326"
    city_df_sf = city_df_sf[["ZCTA5CE10", "geometry", "week", "zipcode", "Time Period", "count"]]
    city_df_sf["x"] = city_df_sf.centroid.map(lambda p: p.x)
    city_df_sf["y"] = city_df_sf.centroid.map(lambda p: p.y)
    zips_center_x = (max(city_df_sf["x"])+min(city_df_sf["x"]))/2
    zips_center_y = (max(city_df_sf["y"])+min(city_df_sf["y"]))/2
    
    norm = matplotlib.colors.Normalize(vmin = low_end, vmax = high_end, clip=False)
    mapper = cm.ScalarMappable(norm = norm, cmap = cm.cool)
    n_weeks = len(city_df_sf["week"].unique())
    fig, ax = plt.subplots(1, figsize = (n_weeks * 3, n_weeks * 4))
    gs = matplotlib.gridspec.GridSpec(n_weeks, 2, wspace = 0.25, hspace = 0.25, top = 0.95, bottom = .05, figure = fig)
    
    ax.axis('off')
    axinset1 = inset_axes(ax, loc='center right', borderpad=-7, 
                      width="2%", height="40%")
    cb = plt.colorbar(cax=axinset1, mappable=mapper)
    cb.set_label("Device Count", labelpad=20)
    
    plt.suptitle('Downtown ' + city_name + '\'s Device Count Over Time', fontsize=16) 
    
    plot_i = 0
    for idx, week in enumerate(city_df_sf["week"].unique()):
        week_i = idx + 1
        for idx2, count_type in enumerate(city_df_sf["Time Period"].unique()):
            ax = fig.add_subplot(gs[idx, idx2])
            ax.set_title("{}".format(pd.to_datetime(week).strftime("%Y-%m-%d")) + count_type)
            plot_df = city_df_sf.loc[(city_df_sf["week"] == week) & 
                                     (city_df_sf["Time Period"] == count_type), :]
            plot_df["center"] = plot_df["geometry"].centroid
            plot_points = plot_df.copy()
            plot_points.set_geometry("center", inplace = True)
            plot_df.plot(ax = ax, color = mapper.to_rgba(plot_df["count"]), edgecolor = "k", missing_kwds= dict(color = "lightgrey", edgecolor = "red", hatch = "///", label = "NaN count"))
            counts = []
            for x, y, count_label in zip(plot_points.geometry.x, plot_points.geometry.y, plot_points["count"]):
                counts.append(plt.text(x, y, count_label, fontsize = 12))
            ax.axis('tight')
            ax.axis('off')
            
            ax.margins(.25,.25, tight = None)
            plot_i = plot_i + 1
    #ax.axis('tight')
    ax.axis('off')
    plt.show()
# uncomment this line to see the plot according to the city selected in the widget.
cityplot(dbutils.widgets.get("City"))

# COMMAND ----------

def lq_tables(city_name, drop_nas = False, drop_outliers = False, show_months = False):
    
    ## moot point
    return

# COMMAND ----------

# what is the weekly lq for a downtown area
sink_city_zipcode_counts_downtown = sink_city_zipcode_counts[sink_city_zipcode_counts["zipcode"].isin(downtown_zipcodes)]
weekly_totals = sink_city_zipcode_counts_downtown.groupby("week").sum().reset_index()
city_df_downtown = sink_city_zipcode_counts_downtown[sink_city_zipcode_counts_downtown["city"] == dbutils.widgets.get("City")]

# isolate downtown areas, then group by week
downtown_city_df_totals = city_df_downtown.groupby("week").sum().reset_index()

# calculate weekly lqs
all_df = weekly_totals.merge(downtown_city_df_totals, on = "week", how = "inner", suffixes = ("_total", "_dwntwn"))
all_df["lq"] = (all_df["covid_vc_zipcode_count_dwntwn"] / all_df["covid_vc_zipcode_count_total"]) / (all_df["precovid_vc_zipcode_count_dwntwn"] / all_df["precovid_vc_zipcode_count_total"])

# lqs of the results week?
city_results_week = results.loc[results["city"] == dbutils.widgets.get("City"), "week"].unique()
all_df[(all_df["week"].isin(city_results_week)) & ((all_df["lq"] > 10) | (all_df["lq"] < 0))]

# COMMAND ----------

# what is the weekly lq for a downtown area, dropping NaNs
sink_city_zipcode_counts_downtown_no_na = sink_city_zipcode_counts_downtown.dropna()
weekly_totals_no_na = sink_city_zipcode_counts_downtown_no_na.groupby("week").sum().reset_index()
city_df_downtown_no_na = sink_city_zipcode_counts_downtown_no_na[sink_city_zipcode_counts_downtown_no_na["city"] == dbutils.widgets.get("City")]

# isolate downtown areas, then group by week
downtown_city_df_totals_no_na = city_df_downtown_no_na.groupby("week").sum().reset_index()

# calculate weekly lqs
all_df_no_na = weekly_totals_no_na.merge(downtown_city_df_totals_no_na, on = "week", how = "inner", suffixes = ("_total", "_dwntwn"))
all_df_no_na["lq"] = (all_df_no_na["covid_vc_zipcode_count_dwntwn"] / all_df_no_na["covid_vc_zipcode_count_total"]) / (all_df_no_na["precovid_vc_zipcode_count_dwntwn"] / all_df_no_na["precovid_vc_zipcode_count_total"])

# lqs of the results week?
city_results_week = results.loc[results["city"] == dbutils.widgets.get("City"), "week"].unique()
all_df_no_na[(all_df_no_na["week"].isin(city_results_week)) & ((all_df_no_na["lq"] > 10) | (all_df_no_na["lq"] < 0))]

# COMMAND ----------

# drop the weeks with unusually low counts for either time period, then calculate the lq for the remaining weeks.
all_df_no_outliers = all_df_no_na[(np.abs(stats.zscore(all_df_no_na[['precovid_vc_zipcode_count_total','covid_vc_zipcode_count_total','precovid_vc_zipcode_count_dwntwn', 'covid_vc_zipcode_count_dwntwn']])) < 3).all(axis=1)]
# any sinks left?
all_df_no_outliers[(all_df_no_outliers["week"].isin(city_results_week)) & (all_df_no_outliers["lq"] > 10) | (all_df_no_outliers["lq"] < 0)]

# COMMAND ----------

# after dropping the nas, there are still some counts that are unusually low given the fact that these are downtown areas- especially for precovid counts 
# look at the months surrounding the unusual counts to see if these are truly outliers in time or part of a larger, strange pattern
# include NaNs to see if there are any months where a zipcode is especially plagued by NaNs/unusually low single digit counts
unusual_months = all_df_no_outliers[(all_df_no_outliers["week"].isin(city_results_week)) & ((all_df_no_outliers["lq"] > 10) | (all_df_no_outliers["lq"] < 0))]["week"].str[:7]
city_df_downtown[city_df_downtown["week"].apply(lambda wk : np.any([wk.startswith(months) for months in unusual_months]))].sort_values(by = ["week", "zipcode"])

# COMMAND ----------

# ignore all subsequent cells for now; scratch work
# these are the number of zip codes in the sink cities sorted alphabetically; use these to determine the col_wrap variable in the facetgrid
#cities = sink_city_zipcode_counts[sink_city_zipcode_counts["city"].isin(sink_cities)].groupby(["city", "week"]).size().index.get_level_values(0).unique()
#unique_zips = sink_city_zipcode_counts[sink_city_zipcode_counts["city"].isin(sink_cities)].groupby(["city", "week"]).size().unique()
#pd.DataFrame(index = cities, data = unique_zips).to_dict()[0]
#sns.set_style("darkgrid")
#colwraps = {"Charlotte" : 9, "Dallas" : 11, "Miami" : 9, "Sacramento" : 11, "Salt Lake City" : 7}
#city_df = sink_city_zipcode_counts[sink_city_zipcode_counts["city"] == city_name]
#print(sink_downtowns[sink_downtowns["_c0"] == dbutils.widgets.get("City")])
#g = sns.FacetGrid(city_df, col = "zipcode", col_wrap = colwraps[dbutils.widgets.get("City")], hue = "Time Period")
#g.map(sns.scatterplot, "week", "count")
#g.add_legend()
#g.fig.subplots_adjust(top = .9)
#for axes in g.axes.flat:
#    _ = axes.set_xticklabels(axes.get_xticklabels(), rotation=45)
#g.fig.suptitle(dbutils.widgets.get("City"));
#plt.tight_layout()

# COMMAND ----------

#zipcode_counts_downtown = {}
#for city in sink_cities:
#    city_df = zipcode_counts[city]
#    downtown_zips = sink_index.loc[sink_index["0"] == city, "downtown_zipcodes"]
#    zipcode_counts_downtown[city] = city_df.loc[city_df["zipcode"].isin(downtown_zips.values[0]), :]

# COMMAND ----------

 # a look at all zipcodes
# sink_dfs is for the sake of easy plotting/indexing
#sink_dfs = []
# outlier_zips is a dictionary where the keys are cities and items are zip codes within that city that have an usually large change between months wrt their precovid/covid counts
# unusually large == more than 2 std devs of a change between months, with respect to the overall changes between months in that specific zip code
#outlier_zips = {}
# outlier_weeks is a dictionary where the keys are cities and items are months where any zip code in 'outlier_zips' has an unusually large change between months
#outlier_weeks = {}
# city_stats is to store dfs to be concatenated together, can be indexed into with the outlier zips dictionary
#city_stats = {}
# diffs_df will become a df of monthly differences, grouped by city by zip by month
#diffs_df = []
#for city in sink_cities:
#    city_df = zipcode_counts_downtown[city]
#    city_df["week"] = pd.to_datetime(city_df["week"])
#    sink_dfs.append(city_df.groupby(["week", "zipcode"]).sum().reset_index())
#    city_stats[city] = []
#    outlier_zips[city] = []
#    outlier_weeks[city] = []
#    for zips in city_df["zipcode"].unique():
#        # group zip codes by month, then see which months had unusual spikes
#        zip_df = city_df.loc[city_df["zipcode"] == zips, :]
#        zip_df = zip_df.groupby(pd.Grouper(key = "week", freq = "1M")).sum()
#        months = zip_df.index[:-1]
#        monthly_diffs = pd.DataFrame({"precovid_vc_count_monthly_diff" : np.diff(zip_df["precovid_vc_zipcode_count"]), "covid_vc_count_monthly_diff" : np.diff(zip_df["covid_vc_zipcode_count"])}, index = months)
#        # std dev for change between months throughout the year
#        precovid_monthly_change_sd = np.std(monthly_diffs["precovid_vc_count_monthly_diff"])
#        covid_monthly_change_sd = np.std(monthly_diffs["covid_vc_count_monthly_diff"])
        # zip codes with unusually large month to month changes
#        if (sum(monthly_diffs["precovid_vc_count_monthly_diff"] > (2 * precovid_monthly_change_sd)) + sum(monthly_diffs["covid_vc_count_monthly_diff"] > (2 * covid_monthly_change_sd))):
#            outlier_zips[city].append(zips)
#            significant_months = months[(monthly_diffs["precovid_vc_count_monthly_diff"] > (2 * precovid_monthly_change_sd)) | (monthly_diffs["covid_vc_count_monthly_diff"] > (2 * covid_monthly_change_sd))].values
#            for sig_month in significant_months:
#                formatted_date = pd.to_datetime(sig_month).strftime("%Y-%m")[:8]
#                if formatted_date not in outlier_weeks[city]:
#                    outlier_weeks[city].append(formatted_date)
#        city_stats[city].append(monthly_diffs)
#    city_stats[city] = pd.concat(city_stats[city], keys = city_df["zipcode"].unique())
#    diffs_df.append(city_stats[city])
#diffs_df = pd.concat(diffs_df, keys = sink_cities)

# COMMAND ----------

# a look at the NAs
#sink_dfs_na = []
#city_stats_na = {}
#for city in sink_cities:
#    city_df = zipcode_counts[city]
#    city_df = city_df[city_df["precovid_vc_zipcode_count"].isna() | city_df["covid_vc_zipcode_count"].isna()]
#    city_df["week"] = pd.to_datetime(city_df["week"])
#    weeks = pd.Series(city_df["week"].unique()[:-1])
#    city_stats_na[city] = []
#    sink_dfs_na.append(city_df.groupby(["week", "zipcode"]).sum().reset_index())
#    for zips in city_df["zipcode"].unique():
#        zips_df_na = city_df.loc[city_df["zipcode"] == zips, :]
#        na_weeks = zips_df_na["week"]
#        city_stats_na[city].append({zips : na_weeks})

# COMMAND ----------

# zip codes that have zero counted devices over the entire time period
# ('Salt Lake City', '84144'): it seems that this is a ZIP code reserved for the post office/PO boxes (https://www.unitedstateszipcodes.org/84144/, + there is a post office on this block)
# ('Orlando', '32839') : does not quite seem to be downtown orlando  https://www.unitedstateszipcodes.org/32839/
# ('Oklahoma City', '73130') is this downtown OKC?
# ('Cincinnati', '45202') this seems to be the entirety of downtown cincinatti
# ('Dallas', '75212')
# ('Dallas', '75115')
# ('Dallas', '75075')
# ('Dallas', '75052')
# ('Charlotte', '28202')
# ('Charlotte', '28027')
# ('Phoenix', '85282')
# ('Phoenix', '85044')
# ('Phoenix', '85308')
# ('Phoenix', '85251')
# the dallas zip code seems to be sparsely populated by POIs - is it 'downtown'
# ^^^ same case with phoenix
# count_medians.index[np.any(count_medians == 0, axis = 1)]

# COMMAND ----------

# todo
# consider just using this and the outlier weeks to plot as reflines on the entire dataset 
# g = sns.FacetGrid(sink_dfs_na[city_idx], col = "zipcode", col_wrap = 5)
# g.map(sns.lineplot, "week", "precovid_vc_zipcode_count", color = 'blue')
# g.map(sns.lineplot, "week", "covid_vc_zipcode_count", color = 'red')
# g.fig.subplots_adjust(top = .9)
# g.fig.suptitle(city_name)

# COMMAND ----------

# a look at the outliers, excluding changes between NA weeks
#sink_dfs = []
#city_stats = {}
#for city in sink_cities:
#    city_df = zipcode_counts[city]
#    city_df["week"] = pd.to_datetime(city_df["week"])
#    city_stats[city] = []
#    city_df = city_df.dropna(subset = ["precovid_vc_zipcode_count", "covid_vc_zipcode_count"], how = "all")
#    sink_dfs.append(city_df.groupby(["week", "zipcode"]).sum().reset_index())
#    for zips in city_df["zipcode"].unique():
#        zips_df = city_df.loc[city_df["zipcode"] == zips, :]
#        pre_covid_diff = np.diff(zips_df["precovid_vc_zipcode_count"])
#        covid_diff = np.diff(zips_df["covid_vc_zipcode_count"])
#        pre_covid_diff_sd = np.std(pre_covid_diff)
#        covid_diff_sd = np.std(covid_diff)
#        weeks = zips_df["week"].unique()[:-1]
#        outlier_weeks = weeks[(abs(pre_covid_diff) > 2 * pre_covid_diff_sd) | (abs(covid_diff) > 2 * covid_diff_sd)]
#        city_stats[city].append({zips : outlier_weeks})

# COMMAND ----------

# todo
# g = sns.FacetGrid(sink_dfs[city_idx], col = "zipcode", col_wrap = 5)
# g.map(sns.lineplot, "week", "precovid_vc_zipcode_count", color = 'blue')
# g.map(sns.lineplot, "week", "covid_vc_zipcode_count", color = 'red')
# g.fig.subplots_adjust(top = .9)
# g.fig.suptitle(city_name)


# COMMAND ----------

# within cities, are there zipcodes that are 'sinkier' than others?
# includes NAs
#problem_weeks_city = {}
#problem_weeks_zip = {}
#for city in sink_city_zipcode_counts["city"].unique():
#    city_df = zipcode_counts[city]
#    pre_covid_diff = np.diff(city_df.groupby("week").sum()["precovid_vc_zipcode_count"])
#    covid_diff = np.diff(city_df.groupby("week").sum()["covid_vc_zipcode_count"])
#    pre_covid_diff_sd = np.std(pre_covid_diff)
#    covid_diff_sd = np.std(covid_diff)
#    weeks = city_df.groupby("week").sum().reset_index()["week"][:-1]
#    problem_weeks_city[city] = weeks[(abs(pre_covid_diff) > 2 * pre_covid_diff_sd) | (abs(covid_diff) > 2 * covid_diff_sd)]
#    problem_weeks_zip[city] = {}
#    for zipcodes in city_df["zipcode"].unique():
#        zipcode_df = city_df.loc[city_df["zipcode"] == zipcodes, :]
#        zipcode_df["week"] = pd.to_datetime(zipcode_df["week"])
#        pre_covid_diff = np.diff(zipcode_df.groupby("week").sum()["precovid_vc_zipcode_count"])
#        covid_diff = np.diff(zipcode_df.groupby("week").sum()["covid_vc_zipcode_count"])
#        pre_covid_diff_sd = np.nanstd(pre_covid_diff[pre_covid_diff != 0])
#        covid_diff_sd = np.nanstd(covid_diff[covid_diff != 0])
#        problem_weeks_zip[city][zipcodes] = weeks[(abs(pre_covid_diff) > 2 * pre_covid_diff_sd) | (abs(covid_diff) > 2 * covid_diff_sd)]

# COMMAND ----------

#problem_weeks_df = pd.DataFrame(problem_weeks_city)
# all weeks that were outliers before or during covid with respect to the entire city
#problem_weeks_df

# COMMAND ----------

# 'outlier' weeks for the given city
#problem_weeks_df[city_name].dropna()

# COMMAND ----------

canadian_city_index = get_table_as_pandas_df('can_city_index_da')

# COMMAND ----------

len(canadian_city_index[canadian_city_index["0"] == "Toronto"]["CMA_da"].values[0])

# COMMAND ----------

canadian_city_index[canadian_city_index["0"] == "Toronto"]["CMA_da"].values[0]

# COMMAND ----------

type(canadian_city_index[canadian_city_index["0"] == "Toronto"]["CMA_da"].values[0])

# COMMAND ----------

len(canadian_city_index[canadian_city_index["0"] == "Toronto"]["CMA_da"].values[0].split(","))

# COMMAND ----------

DAUIDs = [str.strip(dauid)[1:-1] for dauid in canadian_city_index[canadian_city_index["0"] == "Toronto"]["CMA_da"].values[0].split(",")]
DAUIDs[0] = DAUIDs[0][1:]
DAUIDs[-1] = DAUIDs[-1][:-1]

# COMMAND ----------

create_and_save_as_table(pd.DataFrame(DAUIDs), 'toronto_DAUIDs')

# COMMAND ----------

