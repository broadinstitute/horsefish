# imports
import argparse
from firecloud import api as fapi
import pandas as pd
from io import StringIO
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages


# DEFINING PULSENET METRICS' THRESHOLDS
# used to draw y axis lines for thresholds in per-organism plots
est_avg_cov_thresholds = {"Listeria": 20,
                          "Campylobacter": 20,
                          "Escherichia": 40,
                          "Salmonella": 30,
                          "Vibrio": 40}

contig_thresholds = {"Listeria": 100,
                     "Campylobacter": 200,
                     "Escherichia": 600,
                     "Salmonella": 400,
                     "Vibrio": 200}

assembly_thresholds = {"Listeria": {"min": 2800000, "max": 3100000}, 
                       "Campylobacter": {"min": 1400000, "max": 2200000}, 
                       "Escherichia": {"min": 4200000, "max": 5900000}, 
                       "Salmonella": {"min": 4400000, "max": 5600000}, 
                       "Vibrio": {"min": 3800000, "max": 5300000}}

mean_q_thresholds = {"Listeria": 30,
                     "Campylobacter": 30,
                     "Escherichia": 30,
                     "Salmonella": 30,
                     "Vibrio": 30}


def write_plots_to_file(filename, figs):
    """Write plots to file."""
    
    p = PdfPages(filename) 
    # fig_nums = plt.get_fignums()
    # figs = [plt.figure(n) for n in fig_nums]
    
    for fig in figs:  
        
        # and saving the files 
        fig.savefig(p, format="pdf", bbox_inches="tight")  
      
    # close the object 
    p.close()
    plt.close("all")
    
    print(f"All QC visualizations have been written to {filename}.")
    return filename


# color: red; font-weight: bold = additional formatting for cells
# list of colors https://datascientyst.com/full-list-named-colors-pandas-python-matplotlib/
def colorize_q_scores(column):

    return ['background-color: green' if val >= 30
            else 'background-color: pink' if val < 30
            else 'background-color: silver' for val in column]
       
                    
def colorize_metric_qc_chart(df, metric):
    """Color cells in columns for excel sheet based on pass/fail/null."""

    metric_bool_col = f"{metric}_sample_pass"
    success = "background-color: green"
    fail = "background-color: pink"
    nan = "background-color: silver"

    # set everything to NaN color silver
    color_df = pd.DataFrame(nan, index=df.index, columns=[metric])

    # based on boolean value, if they exist, set background color
    if True in df[metric_bool_col].values:
        color_df.loc[df[metric_bool_col]==True, metric] = success
    
    if False in df[metric_bool_col].values:
        color_df.loc[df[metric_bool_col]==False, metric] = fail

    return color_df


def calculate_summary_stats(df):
    """Calculate stats - number of pass, fail, and null samples per metric collected."""

    stats_dict = {}

    for col in df.columns:
        # get counts of True/False/NaN or set to 0
        if True in df[col].values:
            success = df[col].value_counts().loc[True]
        else:
            success = 0
        
        if False in df[col].values:
            fail = df[col].value_counts().loc[False]
        else:
            fail = 0

        nans = (len(df.index)) - success - fail

        # remove the _sample_pass to just get back metric name
        new_column = col.split("sample")[0].strip("_")
        # add pass/fail counts to dictionary
        stats_dict[new_column] = {"success": success, "fail": fail, "null": nans}
        
    # convert dict to df
    stats_df = pd.DataFrame.from_dict(stats_dict)

    return stats_df


# function for plotting threshold lines for each subplot
# implemented from https://stackoverflow.com/questions/48806528/python-seaborn-facetgrid-dynamic-mean-axhline
def plot_threshold_and_color_coverage(organism, x_data, y_data, **kwargs):
    search_key = organism.unique()[0] # "Salmonella"
    res = [val for key, val in est_avg_cov_thresholds.items() if key in search_key]
    threshold = res[0]
    
    # add threshold line
    plt.axhline(threshold, **kwargs)

    # color scatter plot dots based on relation to threshold
    plt.scatter(x_data, y_data, c = y_data.apply(lambda x: "green" if x > threshold else "red"))

    
def plot_threshold_and_color_contigs(organism, x_data, y_data, **kwargs):
    search_key = organism.unique()[0] # "Salmonella"
    res = [val for key, val in contig_thresholds.items() if key in search_key]
    threshold = res[0]

    # add threshold line
    plt.axhline(threshold, **kwargs)

    # color scatter plot dots based on relation to threshold
    plt.scatter(x_data, y_data, c = y_data.apply(lambda x: "red" if x > threshold else "green"))
    
    
def plot_threshold_and_color_assembly(organism, x_data, y_data, **kwargs):
    search_key = organism.unique()[0] # "Salmonella"
    res = [val for key, val in assembly_thresholds.items() if key in search_key]

    threshold_min = res[0]["min"]
    threshold_max = res[0]["max"]

    # add threshold lines
    plt.axhline(threshold_min, **kwargs)
    plt.axhline(threshold_max, **kwargs)

    plt.scatter(x_data, y_data, c = y_data.apply(lambda x: "green" if x in range(threshold_min, threshold_max)
                                                else "red"))


def create_scatter_plot_by_group(data, x_data, y_data, hue_group):
    """Create scatter plots grouped by grouping variable."""

    # create facetgrid for unique values of organism (gambit_predicted_taxon)
    # coverage data by sample BY organism
    scatter_plot_group = sns.FacetGrid(data, col=hue_group, 
                                       height=8, aspect=0.9, col_wrap=2,
                                       sharex=False) # each plot will have its own X values relevant to the plot
    
    
    # map the above facetgrid with actual data from the data table 
    scatter_plot_group.map(sns.scatterplot, x_data, y_data) # , alpha=0.8
    
    # draw threshold lines by subplot in facetgrid and color points based on value above/below threshold lines
    if y_data == "est_coverage_clean":
        scatter_plot_group.map(plot_threshold_and_color_coverage,
                               hue_group, x_data, y_data,
                               c="b", ls="--")

    if y_data == "number_contigs":
        scatter_plot_group.map(plot_threshold_and_color_contigs,
                               hue_group, x_data, y_data,
                               c="b", ls="--")

    if y_data == "assembly_length":
        scatter_plot_group.map(plot_threshold_and_color_assembly,
                               hue_group, x_data, y_data,
                               c="b", ls="--")
    
    return scatter_plot_group


def create_scatter_plot(data, x_data, y_data, hue_group):
    """Create scatter plot given user parameters."""
    
    sns.set_theme(style="darkgrid", palette="muted")
    sns.set("paper", rc = {'figure.figsize':(18, 8)})
    
    # create scatter plot of x and y
    scatter_plot = sns.scatterplot(data=data,
                                   x=x_data,
                                   y=y_data,
                                   hue=hue_group)

    return scatter_plot


def create_failed_sample_chart(df, metric_column, organism_column, sample_id_column, threshold_dict):
    """For a set of thresholds for a metric, get values that fail"""
    
    # subset to specific columns based on metric to visualize and drop NA
    metric_df = df[[sample_id_column, metric_column, organism_column]]#.dropna()
    
    failed_cov_samples = []

    for organism, threshold in threshold_dict.items():

        # subset to rows matching organism from thresholds dict, and rename entity id column
        organism_subset = metric_df[metric_df[organism_column].str.contains(organism)].rename(columns={sample_id_column: "failed_sample_id"})
        
        # create subset df and convert df to dict
        # definition of fail/success samples changes depending on metric
        if metric_column == "est_coverage_clean":
            fails = organism_subset.loc[organism_subset[metric_column] < threshold].to_dict("records")

        if metric_column == "assembly_length":
            threshold_min = threshold["min"]
            threshold_max = threshold["max"]

            fails = organism_subset.loc[~organism_subset[metric_column].between(threshold_min, threshold_max)].to_dict("records")
    
        if metric_column in ["number_contigs", "contigs_fastg", "contigs_gfa"]:
            fails = organism_subset.loc[organism_subset[metric_column] > threshold].to_dict("records")

        if metric_column in ["r1_mean_q_raw", "r2_mean_q_raw"]:
            fails = organism_subset.loc[organism_subset[metric_column] < threshold].to_dict("records")
        
        # append dict to list
        failed_cov_samples.append(fails) # create list of lists

    # flatted list of lists
    failed_cov_samples_fltnd = [sample for organism_subset in failed_cov_samples for sample in organism_subset]
    failed_samples_df = pd.DataFrame(failed_cov_samples_fltnd)

    print(f"There are {failed_samples_df.shape[0]} failed samples for the metric: {metric_column}. \n")
    return failed_samples_df


def get_entity_data(ws_project, ws_name, sample_list, entity_table_name, sample_id_col, run_id=None):
    """Retrieve table data derived by user inputs into dataframe for plotting."""
    
    # get all data from table into df
    # checking a direct read of the table with fiss without the pagination code to first create the .tsv
    # TODO: may need pagination if table has many thousands of rows
    response = fapi.get_entities_tsv(ws_project, ws_name, entity_table_name, model="flexible")
    response_df = pd.read_csv(StringIO(response.text), sep="\t")

    # filter to relevant columms
    full_entity_df = response_df[[sample_id_col, "gambit_predicted_taxon",
                                     "r1_mean_q_raw", "r2_mean_q_raw",
                                     "number_contigs", #"contigs_fastg", "contigs_gfa",
                                     "assembly_length", "est_coverage_clean"]]

    # filter to list of samples provided
    fltrd_entity_df = full_entity_df[full_entity_df[sample_id_col].isin(sample_list)]
    # drop rows where organism is empty  
    fltrd_entity_df.dropna(subset=["gambit_predicted_taxon"], inplace=True)

    # filter df further to specific subset by run_id, if one exists
    if run_id:
        run_entity_df = fltrd_entity_df.loc[fltrd_entity_df["run_id"] == run_id]
        print(f"{run_entity_df.shape[0]} rows gathered from {entity_table_name} that match user criteria.")
        return run_entity_df
    
    print(f"Samples with NULL gambit_predicted_taxon are dropped.")
    print(f"{fltrd_entity_df.shape[0]} rows gathered from {entity_table_name} that match user criteria.")
    
    return fltrd_entity_df


def plot_metric_qc_visualizations(data, sample_names, group_column, threshold_dict, metric_name, label):
    """Plot estimated coverage data."""
            
    # PLOT ALL SAMPLES ALL ORGANISMS
    all_organisms = create_scatter_plot(data, sample_names, metric_name, group_column)
    # set labels
    all_organisms.set_title(f"{label} per Sample", size = 14, fontweight='bold')
    all_organisms.set_xlabel("Sample ID", size = 14)
    all_organisms.set_ylabel(label, size = 14)
    all_organisms.legend(title='Organism', loc='upper right')
    # set ticks
    all_organisms.tick_params(labelrotation=90)


    # PLOT TABLE OF FAILED SAMPLES
    failed_samples = create_failed_sample_chart(data, metric_name, group_column, sample_names, threshold_dict)

    # if there are any failed samples in df - create failed samples chart
    if not failed_samples.empty:
        # add a table at the bottom of the axes to show failed samples
        fig, ax = plt.subplots()
        ax.axis('tight')
        ax.axis('off')
        failed_sample_table = ax.table(cellText=failed_samples.values,
                                                rowLabels=failed_samples.index,
                                                colLabels=failed_samples.columns)


    # PLOT ALL SAMPLES PER ORGANISMS
    per_organism = create_scatter_plot_by_group(data, sample_names, metric_name, group_column)
    # set labels
    per_organism.fig.suptitle(f"{label} per Sample by Organism", size=14, fontweight="bold", y=1.0)
    per_organism.tick_params(labelrotation=90)
    per_organism.set_ylabels(label)
    per_organism.set_xlabels("Sample ID")
    # automatically space between rows of subplots of FacetGrid
    per_organism.fig.tight_layout()
    
    # get figures
    figures = [plt.figure(n) for n in plt.get_fignums()]
    plt.close("all")

    return figures


def create_colorized_scores_table(samples_df):
    """Create chart containing score values and colored by success or fail."""

    # TODO: figure out how to hide the pass columns which are all highlighted red in the excel - styler doesnt support hide when using to_excel()
    colors_df = (samples_df.style
                           .apply(colorize_metric_qc_chart, subset=["est_coverage_clean", "est_coverage_clean_sample_pass"], metric="est_coverage_clean", axis=None)
                           .apply(colorize_metric_qc_chart, subset=["number_contigs", "number_contigs_sample_pass"], metric="number_contigs", axis=None)
                        #    .apply(colorize_metric_qc_chart, subset=["contigs_fastg", "contigs_fastg_sample_pass"], metric="contigs_fastg", axis=None)
                        #    .apply(colorize_metric_qc_chart, subset=["contigs_gfa", "contigs_gfa_sample_pass"], metric="contigs_gfa", axis=None)
                           .apply(colorize_metric_qc_chart, subset=["assembly_length", "assembly_length_sample_pass"], metric="assembly_length", axis=None)
                           .apply(colorize_q_scores, subset=["r1_mean_q_raw", "r2_mean_q_raw"]))
    
    counts_df = calculate_summary_stats(samples_df[["est_coverage_clean_sample_pass",
                                                    "number_contigs_sample_pass",
                                                    "assembly_length_sample_pass",
                                                    "r1_mean_q_raw_sample_pass",
                                                    "r2_mean_q_raw_sample_pass"]])
                                                    # "contigs_fastg_sample_pass",
                                                    # "contigs_gfa_sample_pass"]])

    with pd.ExcelWriter("Colorized_Scores.xlsx") as writer:  
        colors_df.to_excel(writer, sheet_name="all_metrics")
        counts_df.to_excel(writer, sheet_name="summary_stats")

    print(f"Colorized chart written to Colorized_Scores.xlsx.")
    return colors_df


def get_threshold_values_df(table_df, qc_metric_info, sample_id_col):
    """Create wider chart of metrics colored by pass or fail."""

    # get True/False values for pulsenet metric columns with per organism thresholds
    for metric, metric_info in qc_metric_info.items():
        print(f"Starting with {metric}")
        metric_thresholds_dict = metric_info["thresholds_dict"] # dict of thresholds for single metric
    
        # get list of the failed samples and create new column with pass/fail
        failed_samples_df = create_failed_sample_chart(table_df, metric, "gambit_predicted_taxon", sample_id_col, metric_thresholds_dict)

        failed_sample_ids = []
        # check if the dataframe is empty - any failed samples returned or not
        if not failed_samples_df.empty:          
            failed_sample_ids = failed_samples_df["failed_sample_id"].tolist()

        # populate columns with True, False, or NaN if no values exist
        all_sample_ids = table_df[sample_id_col].tolist()
        nan_sample_ids = table_df.loc[table_df[metric].isnull(), sample_id_col].tolist()
        succeeded_sample_ids = list(set(all_sample_ids) - set(nan_sample_ids) - set(failed_sample_ids))

        # if sample_id in list of failed_sample_ids, set to False
        table_df.loc[table_df[sample_id_col].isin(failed_sample_ids), f"{metric}_sample_pass"] = False
        # if sample_id in list of succeeded_sample_ids, set to True
        table_df.loc[table_df[sample_id_col].isin(succeeded_sample_ids), f"{metric}_sample_pass"] = True

    return table_df


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="create visualizations")

    parser.add_argument("-s", "--samples", required=True, type=str, nargs="+", help="list of entity sample_id values")
    parser.add_argument("-t", "--table_name", required=True, type=str, help="name of data table to pull sample data for viz")
    parser.add_argument("-p", "--workspace_project", required=True, help="terra workspace project of viz data")
    parser.add_argument("-w", "--workspace_name", required=True, help="terra workspace name of viz data")
    parser.add_argument("-r", "--run_id", required=False, default=None, help="run_id to filter data from table for viz")
    parser.add_argument("-g", "--grouping_col", required=False, default="gambit_predicted_taxon", help="name of column used for hue/grouping - ie. organism")
    parser.add_argument("-o", "--outfilename", required=False, default="QC_visualizations.pdf", help="name of output pdf file containing visualizations")
    
    args = parser.parse_args()

    # define metric, plot labels, and pulsenet metric thresholds
    qc_metric_info = {
                        "est_coverage_clean": {"label": "Estimated Coverage",
                                               "thresholds_dict": est_avg_cov_thresholds},
                        "number_contigs": {"label": "Number Contigs",
                                           "thresholds_dict": contig_thresholds},
                        "assembly_length": {"label": "Assembly Length",
                                            "thresholds_dict": assembly_thresholds},
                        "r1_mean_q_raw": {"label": "R1 Q Score",
                                          "thresholds_dict": mean_q_thresholds},
                        "r2_mean_q_raw": {"label": "R2 Q Score",
                                          "thresholds_dict": mean_q_thresholds} #,
                        # "contigs_fastg": {"label": "Contigs Fastg",
                        #                   "thresholds_dict": contig_thresholds},
                        # "contigs_gfa": {"label": "Contigs Gfa",
                        #                 "thresholds_dict": contig_thresholds}
                    }
    
    # name of column in terra data table that holds sample_id values - root entity id col
    sample_id_col = f"entity:{args.table_name}_id" 

    # get dataframe with selected subset of data for plotting - filtered down to relevant columns
    table_df = get_entity_data(args.workspace_project, args.workspace_name, args.samples, args.table_name, sample_id_col, args.run_id)

    # append columns to df - contain true/false values 
    updated_df = get_threshold_values_df(table_df, qc_metric_info, sample_id_col)

    # CREATE COLORIZED CHART
    color_df = create_colorized_scores_table(updated_df)

    # CREATE PULSENET METRIC BASED PLOTS    
    # isolate metrics that require plots
    # TODO: parameterize the list of plot metrics
    plot_qc_metric_info = {key: val for key, val in qc_metric_info.items() if key in ["assembly_length", "number_contigs", "est_coverage_clean"]}

    all_figures = []    # list to hold all figures from all metric plots
    # generate plot figures for all metrics
    for metric_col_name, metric_details in plot_qc_metric_info.items():
        metric_plot_label = qc_metric_info[metric_col_name]["label"]
        metric_thresholds = qc_metric_info[metric_col_name]["thresholds_dict"]

        print(f"Plotting {metric_plot_label} \n\t\t y-axis = {metric_col_name} \n\t\t x-axis = {sample_id_col} \n\t\t hue = {args.grouping_col} (column for grouping/coloring)")
        metric_figs = plot_metric_qc_visualizations(table_df, sample_id_col, args.grouping_col, metric_thresholds, metric_col_name, metric_plot_label)

        # combine all figures from metrics into single list
        all_figures[len(all_figures):] = metric_figs

    # combine the figures from all 3 metric plots and write to PDF file
    write_plots_to_file(args.outfilename, all_figures)
