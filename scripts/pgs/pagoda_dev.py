from firecloud import api as fapi # data import
import plotly.express as px # visualizations!
import pandas as pd # dataframe compatibility
from io import StringIO # reading unreadable data
import argparse # code functionality

# Visualization logic for a single plot!
def create_scatter_plot(data, sample_names, group_column, metric_name, label):
    """Plot estimated coverage data using Plotly."""

    # Create the scatter plot
    scatter_plot = px.scatter(data_frame=data,
                              x=sample_names,
                              y=metric_name,
                              color=group_column,
                              title=f"{label} per Sample",
                              labels={sample_names: "Sample ID", metric_name: label},
                              template="plotly")
    
    # Update layout for plot
    scatter_plot.update_layout(
        title=f"{label} per Sample",
        xaxis_title="Sample ID",
        yaxis_title=label,
        legend_title='Organism',
    )
    
    # Update axes properties (e.g., tick angle)
    scatter_plot.update_xaxes(tickangle=90)

    return scatter_plot

# Retrieve specified workspace data
def get_entity_data(ws_project, ws_name, sample_list, entity_table_name):
    
    workspace_datatable = pd.read_csv(StringIO(fapi.get_entities_tsv(ws_project, ws_name, entity_table_name, model="flexible").text), sep="\t")

    # filter to relevant columms (sample_id, predicted species, and relevant metrics for plotting)
    sample_id_column = "entity:" + entity_table_name + "_id"
    filtered_datatable = workspace_datatable[[sample_id_column,
                                "gambit_predicted_taxon",
                                "number_contigs", "assembly_length", "est_coverage_clean"]]

    # filter to list of samples provided
    plot_data = filtered_datatable[filtered_datatable[sample_id_column].isin(sample_list)]

    # drop rows where organism is empty  
    plot_data.dropna(subset=["gambit_predicted_taxon"], inplace=True)
    print(f"Samples with no gambit_predicted_taxon are dropped.")
    
    return plot_data

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="create visualizations")

    parser.add_argument("-s", "--samples", required=True, type=str, nargs="+", help="List of entity sample_id values")
    parser.add_argument("-bp", "--billing_project", required=True, help="Source Terra billing project")
    parser.add_argument("-w", "--workspace_name", required=True, help="Source Terra workspace name")
    parser.add_argument("-dt", "--datatable_name", required=True, type=str, help="Name of source data table for vizualization")
    parser.add_argument("-g", "--grouping_col", required=False, default="gambit_predicted_taxon", help="Name of column used for hue/grouping - ie. organism")
    parser.add_argument("-o", "--output_filename", required=False, default="QC_visualizations.html", help="Name of output HTML file containing visualizations")

    args = parser.parse_args()

    # # define metric, plot labels, and pulsenet metric thresholds
    qc_metric_info = {
                        "est_coverage_clean": "Estimated Coverage",
                        "number_contigs": "Number Contigs",
                        "assembly_length": "Assembly Length",
                    }
    
    # data = get_entity_data("theiagen_pni", "TheiaProk_PNI_Training_DEMO", sample_broad_demo_ids, "broad_demo")
    data = get_entity_data(args.billing_project, args.workspace_name, args.samples, args.datatable_name)

    html_string = ''
    for metric, label in qc_metric_info.items():
        fig = create_scatter_plot(data, "entity:broad_demo_id", "gambit_predicted_taxon", metric, label)
        html_string += fig.to_html(full_html=False, include_plotlyjs='cdn')

    # Write the combined HTML string to a file
    with open(args.output_filename, "w") as file:
        file.write(html_string)