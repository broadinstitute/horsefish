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

# dataframe of organisms and pulsenet standard contig thresholds - used to draw y axis lines for thresholds
contig_thresholds = {"Listeria": 100,
                     "Campylobacter": 200,
                     "Escherichia": 600,
                     "Salmonella": 400,
                     "Vibrio": 200}


# dataframe of organisms and pulsenet standard contig thresholds - used to draw y axis lines for thresholds
assembly_thresholds = {"Listeria": {"min": 2800000, "max": 3100000}, 
                       "Campylobacter": {"min": 1400000, "max": 2200000}, 
                       "Escherichia": {"min": 4200000, "max": 5900000}, 
                       "Salmonella": {"min": 4400000, "max": 5600000}, 
                       "Vibrio": {"min": 3800000, "max": 5300000}}


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
    metric_df = df[[sample_id_column, metric_column, organism_column]].dropna()
    # rename entity id column to failed samples
    metric_df.rename(columns={sample_id_column: "failed_sample_id"}, inplace=True)
    
    failed_cov_samples = []

    for organism, threshold in threshold_dict.items():

        # get rows where organism and threshold value conditions are met
        subset = metric_df[metric_df[organism_column].str.contains(organism)]
        
        # create subset df and convert df to dict
        # definition of fail/success samples changes depending on metric
        if metric_column == "est_coverage_clean":
            subset = subset.loc[subset[metric_column] < threshold].to_dict("records")
            
        if metric_column == "number_contigs":
            subset = subset.loc[subset[metric_column] > threshold].to_dict("records")
            
        if metric_column == "assembly_length":
            threshold_min = threshold["min"]
            threshold_max = threshold["max"]

            subset = subset.loc[~subset[metric_column].between(threshold_min, threshold_max)].to_dict("records")
    
        # append dict to list
        failed_cov_samples.append(subset) # create list of lists

    # flatted list of lists
    failed_cov_samples_ft = [sample for organism_subset in failed_cov_samples for sample in organism_subset]
    failed_samples_df = pd.DataFrame(failed_cov_samples_ft)

    return failed_samples_df


def get_entity_data(ws_project, ws_name, entity_table_name, run_id=None):
    """Retrieve table data derived by user inputs into dataframe for plotting."""
    
    # get all data from table into df
    # checking a direct read of the table with fiss without the pagination code to first create the .tsv
    # TODO: may need pagination if table has many thousands of rows
    response = fapi.get_entities_tsv(ws_project, ws_name, entity_table_name, model="flexible")
    full_entity_df = pd.read_csv(StringIO(response.text), sep="\t") #index_col=f"entity:{table_name}_id"
    
    # filter df to specific subset by run_id, if one exists
    if run_id:
        run_entity_df = full_entity_df.loc[full_entity_df["run_id"] == run_id]
        print(f"{run_entity_df.shape[0]} rows gathered from {entity_table_name} that match user criteria.")
        return run_entity_df
    
    print(f"{full_entity_df.shape[0]} rows gathered from {entity_table_name} that match user criteria.")
    return full_entity_df


def plot_metric_qc_visualizations(data, sample_names, group_column, threshold_dict, metric_name, label):
    """Plot estimated coverage data."""
        
    # PLOT TABLE OF FAILED SAMPLES
    failed_samples = create_failed_sample_chart(data, metric_name, group_column, sample_names, threshold_dict)
    
    # PLOT ALL SAMPLES ALL ORGANISMS
    all_organisms = create_scatter_plot(data, sample_names, metric_name, group_column)
    # set labels
    all_organisms.set_title(f"{label} per Sample", size = 14, fontweight='bold')
    all_organisms.set_xlabel("Sample ID", size = 14)
    all_organisms.set_ylabel(label, size = 14)
    all_organisms.legend(title='Organism', loc='upper right')
    # set ticks
    all_organisms.tick_params(labelrotation=90)

    # add a table at the bottom of the axes to show failed samples
    fig, ax = plt.subplots()#figsize=(11, 8.5)
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
    # est_cov_org.set_axis_labels("Sample ID")
    per_organism.set_ylabels(label)
    per_organism.set_xlabels("Sample ID")
    # automatically space between rows of subplots of FacetGrid
    per_organism.fig.tight_layout()
    
    # get figures
    figures = [plt.figure(n) for n in plt.get_fignums()]
    plt.close("all")

    return figures


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="create visualizations")

    parser.add_argument("-t", "--table_name", required=True, type=str, help="name of data table to use for viz")
    parser.add_argument("-r", "--run_id", required=False, default=None, help="run_id to filter data from table for viz")
    parser.add_argument("-p", "--workspace_project", required=True, help="terra workspace project of viz data")
    parser.add_argument("-w", "--workspace_name", required=True, help="terra workspace name of viz data")
    parser.add_argument("-g", "--grouping_col", required=False, default="gambit_predicted_taxon", help="name of column used for hue/grouping - ie. organism")
    parser.add_argument("-o", "--outfilename", required=False, default="QC_visualizations.pdf", help="name of output pdf file containing visualizations")
    
    args = parser.parse_args()

    # name of column in terra data table that holds sample_id values - root entity id col
    sample_id_col = f"entity:{args.table_name}_id" 

    # get dataframe with selected subset of data for plotting
    table_df = get_entity_data(args.workspace_project, args.workspace_name, args.table_name, args.run_id)
    
    # create plots for pulsenet metrics    
    # list to hold all figures from all metric plots
    all_figures = []
    # define metric, plot labels, and pulsenet metric thresholds
    qc_metric_info = {"est_coverage_clean": {"label": "Estimated Coverage",
                                             "thresholds_dict": est_avg_cov_thresholds},
                      "number_contigs": {"label": "Number Contis",
                                         "thresholds_dict": contig_thresholds},
                      "assembly_length": {"label": "Assembly Length",
                                          "thresholds_dict": assembly_thresholds}
                    }
    
    # generate plot figures for all metrics
    for metric_col_name, metric_details in qc_metric_info.items():
        metric_plot_label = qc_metric_info[metric_col_name]["label"]
        metric_thresholds = qc_metric_info[metric_col_name]["thresholds_dict"]

        print(f"Plotting {metric_plot_label} \n\t\t y-axis = {metric_col_name} \n\t\t x-axis = {sample_id_col} \n\t\t hue = {args.grouping_col} (column for grouping/coloring)")
        metric_figs = plot_metric_qc_visualizations(table_df, sample_id_col, args.grouping_col, metric_thresholds, metric_col_name, metric_plot_label)

        # combine all figures from metrics into single list
        all_figures[len(all_figures):] = metric_figs

    # combine the figures from all 3 metric plots and write to PDF file
    write_plots_to_file(args.outfilename, all_figures)


