# imports
import argparse
from firecloud import api as fapi
import pandas as pd
from io import StringIO
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages


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


def create_scatter_plot(data, x_data, y_data, hue_group):
    """Create scatter plot given user parameters."""
    
    sns.set_theme(style="darkgrid", palette="muted")
    sns.set("paper", rc = {'figure.figsize':(18, 8)})
    
    # create scatter plot of x and y
    scatter_plot = sns.scatterplot(data=data,
                                   x=x_data,
                                   y=y_data,
                                   hue=hue_group,
                                   s=50)

    return scatter_plot


def plot_metric_qc_visualizations(data, sample_names, group_column, metric_name, label):
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

    # get figures
    figures = [plt.figure(n) for n in plt.get_fignums()]
    plt.close("all")

    return figures


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
                                     "number_contigs",
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
                        "est_coverage_clean": "Estimated Coverage",
                        "number_contigs": "Number Contigs",
                        "assembly_length": "Assembly Length",
                    }
    
    # name of column in terra data table that holds sample_id values - root entity id col
    sample_id_col = f"entity:{args.table_name}_id" 

    # get dataframe with selected subset of data for plotting - filtered down to relevant columns
    table_df = get_entity_data(args.workspace_project, args.workspace_name, args.samples, args.table_name, sample_id_col, args.run_id)

    # CREATE PULSENET METRIC BASED PLOTS
    all_figures = []    # list to hold all figures from all metric plots
    for metric_col_name, metric_plot_label in qc_metric_info.items():
        # metric_plot_label = qc_metric_info[metric_col_name]["label"]

        print(f"Plotting {metric_plot_label} \n\t\t y-axis = {metric_col_name} \n\t\t x-axis = {sample_id_col} \n\t\t hue = {args.grouping_col} (column for grouping/coloring)")
        metric_figs = plot_metric_qc_visualizations(table_df, sample_id_col, args.grouping_col, metric_col_name, metric_plot_label)

        # combine all figures from metrics into single list
        all_figures[len(all_figures):] = metric_figs

    # combine the figures from all 3 metric plots and write to PDF file
    write_plots_to_file(args.outfilename, all_figures)
