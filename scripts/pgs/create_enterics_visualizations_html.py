from firecloud import api as fapi # data import
import plotly.express as px # visualizations!
import plotly.graph_objects as go # pretty tables
import pandas as pd # dataframe compatibility
from io import StringIO # reading unreadable data
import argparse # code functionality
import json # data
import numpy as np # dataaaa
from datetime import datetime # date and time functionality
import pytz # also date and timee

# defining pulsenet metrics' thresholds
default_thresholds = {
    "est_coverage_clean": {"Listeria": 20, "Campylobacter": 20, "Escherichia": 40, "Salmonella": 30, "Vibrio": 40},
    "number_contigs": {"Listeria": 100, "Campylobacter": 200, "Escherichia": 600, "Salmonella": 400, "Vibrio": 200},
    "assembly_length": {
        "Listeria": {"min": 2800000, "max": 3100000},
        "Campylobacter": {"min": 1400000, "max": 2200000},
        "Escherichia": {"min": 4200000, "max": 5900000},
        "Salmonella": {"min": 4400000, "max": 5600000},
        "Vibrio": {"min": 3800000, "max": 5300000}
    }
}

# Visualization logic for a single plot!
def create_scatter_plot(data, sample_names, group_column, metric_name, label):

    data = data.sort_values(group_column, ascending=True)

    # Create the scatter plot
    scatter_plot = px.scatter(data_frame=data,
                              x=sample_names,
                              y=metric_name,
                              color=group_column,
                              labels={sample_names: "Sample ID", metric_name: label},
                              template="plotly")

    # Update layout for plot
    scatter_plot.update_layout(
        xaxis_title="Samples",
        yaxis_title=label,
        legend_title='Organism',
        autosize=True,
        margin=dict(l=50, r=50, t=50, b=50)  
    )

    # Update axes properties, removing individual x-axis labels and placing title underneath
    scatter_plot.update_xaxes(tickangle=0, tickfont=dict(color='white',size=1))

    return scatter_plot

# Visualization logic for organism-specific plots
def create_organism_specific_plot(data, sample_names, group_column, metric_name, label, organism, threshold):
    
    # Filter data for the specific organism
    organism_data = data[data[group_column] == organism]

    # Determine pass/fail status based on threshold
    if metric_name == "est_coverage_clean":
        organism_data['Status'] = organism_data[metric_name].apply(lambda x: 'Pass' if x >= threshold else 'Fail')
    elif metric_name == "number_contigs":
        organism_data['Status'] = organism_data[metric_name].apply(lambda x: 'Pass' if x <= threshold else 'Fail')
    else:
        min_thres, max_thres = threshold['min'], threshold['max']
        organism_data['Status'] = organism_data[metric_name].apply(lambda x: 'Pass' if min_thres <= x <= max_thres else 'Fail')

    organism_data.sort_values(by=['Status', sample_names], ascending=[True, True], inplace=True)

    # Create the scatter plot
    scatter_plot = px.scatter(
                                data_frame=organism_data,
                                x=sample_names,
                                y=metric_name,
                                color='Status',
                                color_discrete_map={'Pass': '#67e087', 'Fail': '#ff5c5c'},
                                labels={sample_names: "Sample ID", metric_name: label},
                                template="plotly"
                            )
    
    scatter_plot.update_traces(marker=dict(
            size=7, 
            line=dict(
                width=1, 
                color='black' 
            )
        ))
    
    # Account for metrics that have min and max thresholds
    if metric_name == "assembly_length":

        # Thresholds for assembly_length are expected to be a dictionary
        min_threshold_value = threshold["min"]
        max_threshold_value = threshold["max"]

        # Add shapes for min and max, if they are specified
        scatter_plot.add_shape(type="line", 
                            x0=0, y0=min_threshold_value, x1=1, y1=min_threshold_value, 
                            xref="paper", yref="y",
                            line=dict(color="Red", width=2, dash="dot"))
        
        scatter_plot.add_shape(type="line", 
                            x0=0, y0=max_threshold_value, x1=1, y1=max_threshold_value, 
                            xref="paper", yref="y",
                            line=dict(color="Blue", width=2, dash="dot"))
        
        # Adjust y-axis range
        max_data_value = data[metric_name].max()
        min_data_value = data[metric_name].min()
        y_axis_max = max(max_data_value, max_threshold_value) * 1.1
        y_axis_min = min(min_data_value, min_threshold_value) * 0.9

        # Update layout and axes as before
        scatter_plot.update_layout(
            xaxis_title="Sample ID",
            yaxis_title=label,
            showlegend=False,
            yaxis=dict(range=[y_axis_min, y_axis_max]), 
            autosize=True,
            margin=dict(l=50, r=50, t=50, b=50)  
        )

        scatter_plot.update_xaxes(tickangle=90)

    else:

        # Add threshold line if applicable
        scatter_plot.add_shape(type="line", 
                                x0=0, y0=threshold, x1=1, y1=threshold, 
                                xref="paper", yref="y",
                                line=dict(color="Red", width=3, dash="dashdot"))

        # Adjust y-axis range
        max_data_value = data[metric_name].max()
        y_axis_max = max(max_data_value, threshold) * 1.1  

        # Update layout and axes as before
        scatter_plot.update_layout(
            xaxis_title="Sample ID",
            yaxis_title=label,
            showlegend=False,
            yaxis=dict(range=[0, y_axis_max]),
            autosize=True,
            margin=dict(l=50, r=50, t=50, b=50)  
        )

        scatter_plot.update_xaxes(tickangle=90)

        # Plots with more than 29 samples do not display all labels properly
        if len(organism_data) > 29:
            scatter_plot.update_xaxes(tickangle=0, tickfont=dict(color='white',size=1))

    return scatter_plot

# Creating organism-specific data tables
def create_organism_specific_table(data, sample_names, group_column, metric_name, organism, threshold):

    # Create subsection of only relevant data
    organism_data = data[data[group_column] == organism]

    if metric_name == "est_coverage_clean":
        # Identify failed samples
        failed_samples = organism_data[organism_data[metric_name] < threshold]
        passed_samples = organism_data[organism_data[metric_name] >= threshold]
    
    elif metric_name == "number_contigs":
        # Identify failed samples
        failed_samples = organism_data[organism_data[metric_name] > threshold]
        passed_samples = organism_data[organism_data[metric_name] <= threshold]

    elif metric_name == "assembly_length":
        # Identify failed samples
        failed_samples = organism_data[(organism_data[metric_name] < threshold["min"]) | (organism_data[metric_name] > threshold["max"])]
        passed_samples = organism_data[(organism_data[metric_name] >= threshold["min"]) & (organism_data[metric_name] <= threshold["max"])]

    else:
        return None

    # Do not reset index after concatenation
    combined_samples = pd.concat([failed_samples, passed_samples])

    # Now determine status based on original index
    combined_samples['Status'] = np.where(combined_samples.index.isin(failed_samples.index), 'Fail', 'Pass')

    # Now you can reset the index if you need to
    combined_samples.reset_index(drop=True, inplace=True)

    # Sort alphabetically
    combined_samples.sort_values(by=['Status', sample_names], ascending=[True, True], inplace=True)

    # Add new row to visually denote end of data
    end_of_data_row = {sample_names: '       END', metric_name: '        OF', 'Status': '       DATA'}
    combined_samples = pd.concat([combined_samples, pd.DataFrame([end_of_data_row])], ignore_index=True)

    # Create the table figure
    table_figure = go.Figure(data=[go.Table(
                    header=dict(
                        values=['Sample ID', 'Value', 'Status'],
                        fill_color='#d1d4dc',
                        align='left',
                        line_color='black',
                        font=dict(size=13)),
                    cells=dict(
                        values=[combined_samples[sample_names], combined_samples[metric_name], combined_samples['Status']],
                        fill_color=[
                            # Set background color for entire row based on 'Status'
                            ['#ff5d5d' if status == 'Fail' else ('#e8ece8' if status == '       DATA' else 'white') for status in combined_samples['Status']],
                            ['#ff5d5d' if status == 'Fail' else ('#e8ece8' if status == '       DATA' else 'white') for status in combined_samples['Status']],
                            ['#ff5d5d' if status == 'Fail' else ('#e8ece8' if status == '       DATA' else 'white') for status in combined_samples['Status']]
                        ],
                        align=['left'],   
                        line_color=['black'],
                        font=dict(size=12, color=['black'])
                    ))
    ])

    # Update table layout
    table_figure.update_layout(
        margin=dict(l=15, r=5, t=50, b=0)
    )

    return table_figure

# Create data table with all data
def create_complete_table(data, entity_table_name, group_column):

    sample_id_column = "entity:" + entity_table_name + "_id"
    data = data.sort_values(sample_id_column, ascending=True)

    # Add new row to visually denote end of data
    end_of_data_row = {sample_id_column: ' ', group_column: '                   END', 'number_contigs': '                    OF', 'assembly_length': '                   DATA', 'est_coverage_clean': ' '}
    data = pd.concat([data, pd.DataFrame([end_of_data_row])], ignore_index=True)
    
    # Create the table figure
    table_figure = go.Figure(data=[go.Table(
        header=dict(
            values=[
                'Sample ID',
                'Predicted Taxon',
                'Number of Contigs',
                'Assembly Length',
                'Estimated Coverage'
            ],
            fill_color='#d8d4dc',
            align='left',
            line_color='black',
            font=dict(size=13)
        ),
        cells=dict(
            values=[
                data[sample_id_column],
                data[group_column],
                data["number_contigs"],
                data["assembly_length"],
                data["est_coverage_clean"]
            ],
            align='left',
            fill_color=[
                # Set background color for entire row based on 'Status'
                ['#e8ece8' if status == '                   END' else 'white' for status in data[group_column]],
                ['#e8ece8' if status == '                   END' else 'white' for status in data[group_column]],
                ['#e8ece8' if status == '                   END' else 'white' for status in data[group_column]],
                ['#e8ece8' if status == '                   END' else 'white' for status in data[group_column]],
                ['#e8ece8' if status == '                   END' else 'white' for status in data[group_column]]
            ],
            line_color='black',
            font=dict(size=12)
        )
    )])

    # Update table layout if necessary
    table_figure.update_layout(
        autosize=True,
        margin=dict(l=15, r=5, t=50, b=0)
    )

    return table_figure

# Visualization logic for gambit breakdown plot 
def create_gambit_breakdown_plot(data, group_column, metric_name, thresholds):
   
    # Map organisms to their genus
    data['Genus'] = data[group_column].apply(map_organism_to_genus)

    # Initialize columns for 'Pass', 'Fail', and 'NA'
    data['Pass'] = 0
    data['Fail'] = 0
    data['NA'] = 0

    # Apply thresholds and set statuses
    for index, row in data.iterrows():
        genus = row['Genus']
        value = row[metric_name]
        if genus in thresholds[metric_name]:
            if metric_name == "assembly_length":
                if thresholds[metric_name][genus]['min'] <= value <= thresholds[metric_name][genus]['max']:
                    data.at[index, 'Pass'] = 1
                else:
                    data.at[index, 'Fail'] = 1
            else:
                threshold = thresholds[metric_name][genus]
                if ((metric_name == "est_coverage_clean" and value >= threshold) or
                   (metric_name == "number_contigs" and value <= threshold)):
                    data.at[index, 'Pass'] = 1
                else:
                    data.at[index, 'Fail'] = 1
        else:
            data.at[index, 'NA'] = 1

    # Group by Genus and sum up 'Pass', 'Fail', and 'NA'
    breakdown = data.groupby(group_column).agg({'Pass': 'sum', 'Fail': 'sum', 'NA': 'sum'}).reset_index()

    breakdown = breakdown.sort_values(group_column, ascending=False)

    # Create the stacked bar chart with horizontal bars
    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=breakdown[group_column],
        x=breakdown['Pass'],
        name='Pass',
        marker_color='#67e087',
        orientation='h'
    ))
    fig.add_trace(go.Bar(
        y=breakdown[group_column],
        x=breakdown['Fail'],
        name='Fail',
        marker_color='#ff5c5c',
        orientation='h'
    ))
    fig.add_trace(go.Bar(
        y=breakdown[group_column],
        x=breakdown['NA'],
        name='No Threshold Data',
        marker_color='grey',
        orientation='h'
    ))

    # Update the layout of the plot for horizontal bars
    fig.update_layout(
        barmode='stack',
        yaxis_title='Gambit Predicted Taxon',
        xaxis_title='Count',
        autosize =True,
        showlegend=False,
        margin=dict(l=50, r=50, t=15, b=50),
        xaxis=dict(
            tickfont_size=12, 
        ),
        yaxis=dict(
            tickfont_size=10,  
            automargin=True    
        )
    )

    fig.update_yaxes(automargin=True)

    return fig

# Create data table with gambit breakdown
def create_gambit_breakdown_table(data, group_column, metric_name, thresholds):

    # Initialize columns for 'Pass', 'Fail', and 'NA'
    data['Pass'] = 0
    data['Fail'] = 0
    data['NA'] = 0

    # Apply thresholds and set statuses
    for index, row in data.iterrows():
        organism = row[group_column]
        value = row[metric_name]
        genus = map_organism_to_genus(organism)

        if genus in thresholds[metric_name]:
            if metric_name == "assembly_length":
                # For assembly_length, check if within the min-max range
                if value >= thresholds[metric_name][genus]['min'] and value <= thresholds[metric_name][genus]['max']:
                    data.at[index, 'Pass'] = 1
                else:
                    data.at[index, 'Fail'] = 1
            else:
                # For other metrics, compare against a single threshold value
                if (metric_name == "est_coverage_clean" and value >= thresholds[metric_name][genus]) or \
                   (metric_name == "number_contigs" and value <= thresholds[metric_name][genus]):
                    data.at[index, 'Pass'] = 1
                else:
                    data.at[index, 'Fail'] = 1
        else:
            # Mark as 'NA' if there's no threshold data for the genus
            data.at[index, 'NA'] = 1

    # Group by organism and calculate counts and percentages
    breakdown = data.groupby(group_column).agg(
        Total_Count=(group_column, 'count'),
        Passed_Count=('Pass', 'sum'),
        Failed_Count=('Fail', 'sum'),
        NA_Count=('NA', 'sum')
    ).reset_index()

    # Calculate percentages and convert to string with '%' appended
    breakdown['% Passed Threshold'] = (breakdown['Passed_Count'] / breakdown['Total_Count'] * 100).round(2).astype(str) + '%'
    breakdown['% Failed Threshold'] = (breakdown['Failed_Count'] / breakdown['Total_Count'] * 100).round(2).astype(str) + '%'

    # Replace percentages with 'No Threshold Data' where applicable
    breakdown.loc[breakdown['NA_Count'] > 0, '% Passed Threshold'] = 'No Threshold Data'
    breakdown.loc[breakdown['NA_Count'] > 0, '% Failed Threshold'] = 'No Threshold Data'

    # Create the table figure
    table_figure = go.Figure(data=[go.Table(
        header=dict(
            values=['Gambit Predicted Taxon', 'Count', '% Passed Threshold', '% Failed Threshold'],
            fill_color='#d1d4dc',
            align='center',
            line_color='black',
            font=dict(size=13)
        ),
        cells=dict(
            values=[
                breakdown[group_column],
                breakdown['Total_Count'],
                breakdown['% Passed Threshold'],
                breakdown['% Failed Threshold']
            ],
            fill_color='white',
            align='left',
            height=50,
            line_color='black',
            font=dict(size=12, color='black')
        ))
    ])

    # Update table layout
    table_figure.update_layout(
        margin=dict(l=15, r=15, t=15, b=15)
    )

    return table_figure

# Designate organisms to their respective bacterial genus
def map_organism_to_genus(organism_name):
    if " " in organism_name:
        return organism_name.split()[0]
    else:
        return organism_name.split("_")[0]
    
# Retrieve specified workspace data
def get_entity_data(ws_project, ws_name, sample_list, entity_table_name, group_column):
    
    workspace_datatable = pd.read_csv(StringIO(fapi.get_entities_tsv(ws_project, ws_name, entity_table_name, model="flexible").text), sep="\t")

    # filter to relevant columms (sample_id, predicted species, and relevant metrics for plotting)
    sample_id_column = "entity:" + entity_table_name + "_id"
    filtered_datatable = workspace_datatable[[sample_id_column,
                                group_column,
                                "number_contigs", "assembly_length", "est_coverage_clean"]]

    # filter to list of samples provided
    plot_data = filtered_datatable[filtered_datatable[sample_id_column].isin(sample_list)]

    # drop rows where organism is empty  
    plot_data.dropna(subset=[group_column], inplace=True)
    print(f"Samples with no gambit_predicted_taxon are dropped.")
    
    return plot_data

# Modify default thresholds based on user input
def update_thresholds_from_file(thresholds_file, default_thresholds):
    with open(thresholds_file, 'r') as file:
        custom_thresholds = json.load(file)
    for threshold_type, thresholds in custom_thresholds.items():
        for organism, values in thresholds.items():
            if organism in default_thresholds[threshold_type]:
                print(f"Overriding {threshold_type} threshold for {organism}.")
            else:
                print(f"Adding new {threshold_type} threshold for {organism}.")
            default_thresholds[threshold_type][organism] = values

# Checking if organism has threshold data
def is_organism_in_thresholds(organism, thresholds):

    for metric, values in thresholds.items():
        genus = map_organism_to_genus(organism)
        if genus in values:
            return True
        else:
            return False

# Piecing together all visualizations into HTML string
def create_HTML(data, qc_metric_info, default_thresholds, entity_table_name, grouping_column):
    # Initialize the HTML string with the header, style, and TOC container
    html_string = '''
    <!DOCTYPE html>
    <html>
    <head>
        <link href="https://fonts.googleapis.com/css2?family=Droid+Sans&display=swap" rel="stylesheet">
        <meta charset="UTF-8">
        <title>Sequencing QC Report</title>
        <style>
            body {
                font-family: 'Arial', sans-serif;
                display: flex;
                flex-wrap: nowrap; /* Prevents flex items from wrapping */
            }
            #toc-title {
                font-family: 'Droid Sans', sans-serif;
                text-align: center;
                padding-top: 25px; /* Only top and bottom padding */
                padding-bottom: 10px;
                font-size: 24px;
                line-height: 1.2;
                background-color: #f8f9fa;
                width: 100%; /* Title width matches the TOC container width */
                box-sizing: border-box; /* Include padding and border in the element's total width and height */
            }
            #toc-info {
                font-family: 'Helvetica', sans-serif;
                text-align: center;
                padding-top: 20px 0px;
                padding-bottom: 20px;
                font-size: 13px;
                line-height: 1.5;
                color: #3e506c;
                background-color: #f8f9fa;
                width: 100%; /* Title width matches the TOC container width */
                box-sizing: border-box; /* Include padding and border in the element's total width and height */
            }
            #toc-container {
                width: 250px;
                position: fixed;
                height: 100%;
                overflow-y: auto;
                background-color: #f8f9fa; 
                padding-right: 13px; /* Prevents TOC from overlapping the content */
                padding-left: 3px;
                transition: width 0.5s; /* Smooth transition for TOC width */
                top: 0px;
            }
            ul#toc {
                list-style-type: none; /* Removes bullet points from TOC items */
                padding: 0;
                margin: 0;
                color: #717afc; /* Color for TOC headers */
                font-size: 15px; /* Font size for TOC headers */
            }
            ul#toc > li > a {
                margin-bottom: 20px; /* Adds more space after the last subsection before the next header */
                margin-top: 5px;
            }
            ul#toc li a {
                color: #8193a7; /* Link color for TOC items */
                text-decoration: none; /* Removes underline from links */
                display: block; /* Makes the entire list item clickable */
                padding: 5px 10px; /* Adds padding for touch targets */
                font-size: 14px; /* Font size for TOC items */
            }
            ul#toc li ul li a {
                margin-bottom: 3px; /* Adds spacing after each TOC item */
            }
            ul#toc li ul li:last-child a {
                margin-bottom: 7px; /* Extra space after the last link before the next header */
            }
            ul#toc li ul li:first-child a {
                margin-top: 5px; /* Extra space after the last link before the next header */
            }
            ul#toc li a:hover {
                background-color: #E2E2E2; /* Adds a hover effect for links */
            }
            .collapsible {
                cursor: pointer;
                padding: 5px;
                width: 100%;
                border: none;
                text-align: left;
                outline: none;
                font-size: 15px;
                background-color: #f8f9fa;
                color: #717afc;
            }
            .collapsible:after {
                content: '\\25C2'; /* Down-pointing small triangle */
                font-weight: bold;
                float: right;
                font-family: 'Arial', sans-serif;
            }
            .active:after {
                content: '\\25BE'; /* Right-pointing small triangle */
                font-family: 'Arial', sans-serif;
            }
            .content {
                display: block;
                overflow: hidden;
                background-color: #f8f9fa;
            }
            #content-container {
                margin-left: 250px;
                width: calc(100% - 250px);
                overflow-y: auto;
                padding: 5px 35px;
                color: #404040;
            }
            #content-container h2 {
                font-family: 'Arial', sans-serif;
                font-size: 22px;
                font-weight: bold;
                color: #2a3f5f;
                margin-top: 30px;
                margin-bottom: 0px;
                text-align: center;
            }
            #content-container h3 {
                font-family: 'Arial', sans-serif;
                font-size: 18px;
                font-weight: bold;
                color: #2a3f5f;
                margin-top: 30px;
                margin-bottom: 10px;
                text-align: center;
                font-style: italic;
            }
            #content-container div:first-child {
                margin-top: 0px;
            }
            .section-title h3 {
                text-align: center;
                margin-bottom: 0px; /* Adjust as needed */
            }
            .plot-table-container {
                display: flex;
                flex-direction: row; 
                justify-content: space-between;
                align-items: flex-start;
                margin-bottom: 20px;
                gap: 0px;
                padding: 0px 0px;
            }
            .plot-container {
                flex: 0 0 70%;
                padding: 0px;
            }
            .table-container {
                flex: 1; 
                padding: 0px;
                overflow-x: auto;
                overflow-y: auto;
                scrollbar-width: thin;
                scrollbar-color: #717afc #f8f9fa;
            }
            .gambit-container {
                flex: 0 0 50%;
                padding: 0px;
            }
            .threshold-info {
                text-align: center;
                color: #555;
                font-size: 14px;
                margin-bottom: 10px;
            }
        </style>
    </head>
    <body>
        <div id="toc-container">
            <div id="toc-title">
                Sequencing
                <br>
                QC Report
            </div>
    '''

    # Get current time in UTC
    current_time_utc = datetime.now(pytz.utc)
    # Format the time string in ISO 8601 format and convert to string
    formatted_time = current_time_utc.strftime('at %H:%M:%S UTC')
    formatted_date = current_time_utc.strftime('on %Y-%m-%d')

    html_string += f'''
            <div id="toc-info">
                Produced with Terra 
                <br>
                {formatted_date}
                <br>
                {formatted_time}
            </div>
            <ul id="toc">
    '''

    organisms_in_data = set(data[grouping_column])
    organisms_in_data = sorted(organisms_in_data)

    # Add table of contents elements according to data
    for metric, label in qc_metric_info.items():
        html_string += f'''
                <li>
                    <button class="collapsible active">{label}</button>
                    <div class="content">
                        <ul>
                            <li><a href="#{metric}_total">Total</a></li>
        '''
        for organism in organisms_in_data:
            if is_organism_in_thresholds(organism, default_thresholds):
                html_string += f'<li><a href="#{metric}_{organism.replace(" ", "_")}">{organism}</a></li>'
        html_string += '''
                        </ul>
                    </div>
                </li>
        '''
    html_string += '''
        <li>
            <a href="#all_samples_table">Sample Table</a>
        </li>
    '''
    html_string += f'''
            </ul>
        </div>
    '''

    # Content-container for all plots and tables
    html_string += '<div id="content-container">'

    sample_id_column = "entity:" + entity_table_name + "_id"

    for metric, label in qc_metric_info.items():
        # Section for total metric
        html_string += f'<div id="{metric}_total"><h2>{label} per sample</h2>'
        # Total metric plot code
        fig = create_scatter_plot(data, sample_id_column, grouping_column, metric, label)
        html_string += fig.to_html(full_html=False, include_plotlyjs='cdn', config={'responsive': True})
        # End of total metric section
        html_string += '</div>'

        # After the total metric scatterplot section and before organism-specific sections
        html_string += '''
            <!-- Gambit Breakdown Section -->
            <div id="gambit_breakdown" class="section-container">
                <div class="plot-table-container">
                    <!-- Gambit Breakdown Plot Container -->
                    <div class="gambit-container">
        '''

        # Add the gambit breakdown plot
        plot_fig = create_gambit_breakdown_plot(data, grouping_column, metric, default_thresholds)
        html_string += plot_fig.to_html(full_html=False, include_plotlyjs='cdn', config={'responsive': True})

        html_string += '''
                    </div> <!-- End of Plot Container -->
                    <!-- Gambit Breakdown Table Container -->
                    <div class="table-container">
        '''

        # Add the gambit breakdown table
        table_fig = create_gambit_breakdown_table(data, grouping_column, metric, default_thresholds)
        html_string += table_fig.to_html(full_html=False, include_plotlyjs='cdn')

        html_string += '''
                    </div> <!-- End of Table Container -->
                </div> <!-- End of plot-table-container -->
            </div> <!-- End of Gambit Breakdown Section -->
        '''

        # Organism-specific sections
        for organism in organisms_in_data:
            if is_organism_in_thresholds(organism, default_thresholds):
                genus = map_organism_to_genus(organism)

                threshold_info = default_thresholds[metric].get(genus)
            
                # Start section with TOC anchor id
                html_string += f'<div id="{metric}_{organism.replace(" ", "_")}" class="section-container">'

                # Title for section
                html_string += f'<div class="section-title"><h3>{label} per sample for {organism}</h3></div>'

                # Logic for threshold information for the section
                if metric == "est_coverage_clean": 
                    html_string += f'<div class="threshold-info">Threshold for {genus}: Values greater than {threshold_info} are considered passing. Any values less than {threshold_info} are considered failures.</div>'
                elif metric == "number_contigs":
                    html_string += f'<div class="threshold-info">Threshold for {genus}: Values less than {threshold_info} are considered passing. Any values greater than {threshold_info} are considered failures.</div>'
                else:
                    min_thres = threshold_info.get("min")
                    max_thres = threshold_info.get("max")
                    html_string += f'<div class="threshold-info">Thresholds for {genus}: Minimum {min_thres}, Maximum {max_thres}. Values outside this range are considered failures.</div>'

                # Start the flex container for plot and table
                html_string += '<div class="plot-table-container">'
                
                # Add the plot to its own container if a threshold exists
                html_string += '<div class="plot-container">'
                threshold = default_thresholds[metric][genus]
                fig = create_organism_specific_plot(data, sample_id_column, grouping_column, metric, label, organism, threshold)
                html_string += fig.to_html(full_html=False, include_plotlyjs='cdn', config={'responsive': True})
                html_string += '</div>' # End of plot container
                
                # Add the table to its own container if a threshold exists
                html_string += '<div class="table-container">'
                table_fig = create_organism_specific_table(data, sample_id_column, grouping_column, metric, organism, threshold)
                html_string += table_fig.to_html(full_html=False, include_plotlyjs='cdn')
                html_string += '</div>'  # End of table container

                # Close the flex container
                html_string += '</div>'   

                # Close the section container
                html_string += '</div>'

    # Content for datatable containing all samples
    html_string += f'<div id="all_samples_table"><h2>Sample Table</h2>'
    # #Create table
    complete_table_fig = create_complete_table(data, entity_table_name, grouping_column)
    html_string += complete_table_fig.to_html(full_html=False, include_plotlyjs='cdn')
    html_string += '</div>'  # End of table container
            
    html_string += '</div>' # End of content-container  

    # Close HTML tags
    html_string += '''
        <script>

            /*
            document.addEventListener('DOMContentLoaded', function() {
                // Smooth scrolling for TOC links
                document.querySelectorAll('#toc a').forEach(link => {
                    link.onclick = function(e) {
                        e.preventDefault();
                        let target = document.querySelector(this.getAttribute('href'));
                        if(target) {
                            target.scrollIntoView({behavior: 'smooth'});
                        }
                    };
                });
            });
            */

            document.addEventListener('DOMContentLoaded', function() {
                var coll = document.getElementsByClassName("collapsible");
                for (var i = 0; i < coll.length; i++) {
                    // Initialize the correct display state based on the presence of the 'active' class
                    var content = coll[i].nextElementSibling;
                    if (coll[i].classList.contains("active")) {
                        content.style.display = "block";
                    } else {
                        content.style.display = "none";
                    }

                    // Attach click event listener
                    coll[i].addEventListener("click", function() {
                        this.classList.toggle("active");
                        var content = this.nextElementSibling;
                        if (content.style.display === "block") {
                            content.style.display = "none";
                        } else {
                            content.style.display = "block";
                        }
                    });
                }
            });
        </script>
    </body>
    </html>
    '''

    return html_string

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="create visualizations")

    parser.add_argument("-s", "--samples", required=True, type=str, nargs="+", help="List of entity sample_id values")
    parser.add_argument("-bp", "--billing_project", required=True, help="Source Terra billing project")
    parser.add_argument("-w", "--workspace_name", required=True, help="Source Terra workspace name")
    parser.add_argument("-dt", "--datatable_name", required=True, type=str, help="Name of source data table for vizualization")
    parser.add_argument("-g", "--grouping_col", required=False, default="gambit_predicted_taxon", help="Name of column used for hue/grouping - ie. organism")
    parser.add_argument("-o", "--output_filename", required=False, default="QC_visualizations.html", help="Name of output HTML file containing visualizations")

    parser.add_argument("-t", "--thresholds_file", type=str, help="Path to a JSON file containing custom thresholds")

    args = parser.parse_args()

    # Override default thresholds
    if args.thresholds_file:
        update_thresholds_from_file(args.thresholds_file, default_thresholds)

    # # define metric, plot labels, and pulsenet metric thresholds
    qc_metric_info = {
                        "est_coverage_clean": "Estimated Coverage",
                        "number_contigs": "Number Contigs",
                        "assembly_length": "Assembly Length",
                    }
    
    data = get_entity_data(args.billing_project, args.workspace_name, args.samples, args.datatable_name, args.grouping_col)

    html_string = create_HTML(data, qc_metric_info, default_thresholds, args.datatable_name, args.grouping_col)

    # Write the combined HTML string to a file with UTF-8 encoding
    with open(args.output_filename, "w", encoding='utf-8') as file:
        file.write(html_string)