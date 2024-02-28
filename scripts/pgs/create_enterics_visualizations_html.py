import plotly.express as px # visualizations!
import plotly.graph_objects as go # pretty tables
import pandas as pd # dataframe compatibility
import argparse # code functionality
import json # data
import numpy as np # dataaaa
from datetime import datetime # date and time functionality
import pytz # also date and timee

# Scatterplot with all samples for one metric
def create_scatter_plot(data, metric):

    data = data.sort_values("Gambit Predicted Taxon", ascending=True)

    # Create the scatter plot
    scatter_plot = px.scatter(data_frame=data,
                              x="Sample ID",
                              y=metric,
                              color="Gambit Predicted Taxon",
                              labels={"Sample ID": "Sample ID", metric: metric},
                              template="plotly")

    # Update layout for plot
    scatter_plot.update_layout(
        xaxis_title="Samples",
        yaxis_title=metric,
        legend_title='Organism',
        autosize=True,
        margin=dict(l=50, r=50, t=50, b=50)  
    )

    # Update axes properties, removing individual x-axis labels and placing title underneath
    scatter_plot.update_xaxes(tickangle=0, tickfont=dict(color='white',size=1))

    return scatter_plot

# Pathogen-specific scatterplot with pass/fail status for one metric
def create_organism_specific_plot(data, metric, organism, threshold):
    
    # Filter data for the specific organism
    organism_data = data[data["Gambit Predicted Taxon"] == organism]

    # Determine pass/fail status based on threshold
    if metric == "Estimated Coverage":
        organism_data['Status'] = organism_data[metric].apply(lambda x: 'Pass' if x >= threshold else 'Fail')

    elif metric == "Number Contigs":
        organism_data['Status'] = organism_data[metric].apply(lambda x: 'Pass' if x <= threshold else 'Fail')

    else:
        min_thres, max_thres = threshold['min'], threshold['max']
        organism_data['Status'] = organism_data[metric].apply(lambda x: 'Pass' if min_thres <= x <= max_thres else 'Fail')

    # Sort values by pass/fail status
    organism_data.sort_values(by=['Status', "Sample ID"], ascending=[True, True], inplace=True)

    # Create the scatter plot
    scatter_plot = px.scatter(
                                data_frame=organism_data,
                                x="Sample ID",
                                y=metric,
                                color='Status',
                                color_discrete_map={'Pass': '#67e087', 'Fail': '#ff5c5c'},
                                labels={"Sample ID": "Sample ID", metric: metric},
                                template="plotly"
                            )
    
    # Add black outline to scatterplot dots
    scatter_plot.update_traces(marker=dict(
            size=7, 
            line=dict(
                width=1, 
                color='black' 
            )
        ))
    
    # Add two threshold lines for assembly length min/max
    if metric == "Assembly Length":

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
        max_data_value = data[metric].max()
        min_data_value = data[metric].min()
        y_axis_max = max(max_data_value, max_threshold_value) * 1.1
        y_axis_min = min(min_data_value, min_threshold_value) * 0.9

        # Update layout and axes as before
        scatter_plot.update_layout(
            yaxis=dict(range=[y_axis_min, y_axis_max])
        )

    # Add one threshold line for metrics with single threshold
    else:

        # Add threshold line if applicable
        scatter_plot.add_shape(type="line", 
                                x0=0, y0=threshold, x1=1, y1=threshold, 
                                xref="paper", yref="y",
                                line=dict(color="Red", width=3, dash="dashdot"))

        # Adjust y-axis range
        max_data_value = data[metric].max()
        y_axis_max = max(max_data_value, threshold) * 1.1  

        # Update layout and axes as before
        scatter_plot.update_layout(
            yaxis=dict(range=[0, y_axis_max])
        )

    # update scatterplot layout
    scatter_plot.update_layout(
        showlegend=False,
        autosize=True,
        margin=dict(l=50, r=50, t=50, b=50)  
    )
    
    # Plots with more than 29 samples do not display all labels properly, so we hide x axis labels
    if len(organism_data) > 29:
        scatter_plot.update_xaxes(tickangle=0, tickfont=dict(color='white',size=1))
    # Plots with less than 29 samples display x axis labels
    else:
        scatter_plot.update_xaxes(tickangle=90)

    return scatter_plot

# Pathogen-specific table with pass/fail status for one metric
def create_organism_specific_table(data, metric, organism, threshold):

    # Create subsection of only relevant data
    organism_data = data[data["Gambit Predicted Taxon"] == organism]

    # Determine pass/fail status based on threshold

    if metric == "Estimated Coverage":
        failed_samples = organism_data[organism_data[metric] < threshold]
        passed_samples = organism_data[organism_data[metric] >= threshold]
    
    elif metric == "Number Contigs":
        failed_samples = organism_data[organism_data[metric] > threshold]
        passed_samples = organism_data[organism_data[metric] <= threshold]

    elif metric == "Assembly Length":
        failed_samples = organism_data[(organism_data[metric] < threshold["min"]) | (organism_data[metric] > threshold["max"])]
        passed_samples = organism_data[(organism_data[metric] >= threshold["min"]) & (organism_data[metric] <= threshold["max"])]

    # Do not reset index after concatenation
    combined_samples = pd.concat([failed_samples, passed_samples])

    # Determine status based on original index
    combined_samples['Status'] = np.where(combined_samples.index.isin(failed_samples.index), 'Fail', 'Pass')

    # Reset index for ordering purposes
    combined_samples.reset_index(drop=True, inplace=True)

    # Sort alphabetically
    combined_samples.sort_values(by=['Status', "Sample ID"], ascending=[True, True], inplace=True)

    # Add new row to visually denote end of data (note: the extra spaces are for formatting)
    end_of_data_row = {"Sample ID": '       END', metric: '        OF', 'Status': '       DATA'}
    combined_samples = pd.concat([combined_samples, pd.DataFrame([end_of_data_row])], ignore_index=True)

    # Create the table figure
    table_figure = go.Figure(
        data=[
            go.Table(
                header=dict(
                    values=['Sample ID', 'Value', 'Status'],
                    fill_color='#d1d4dc',
                    align='left',
                    line_color='black',
                    font=dict(size=13)
                ),
                cells=dict(
                    values=[combined_samples["Sample ID"], combined_samples[metric], combined_samples['Status']],
                    fill_color=[
                        # Set background color for entire row based on 'Status'
                        ['#ff5d5d' if status == 'Fail' else ('#e8ece8' if status == '       DATA' else 'white') for status in combined_samples['Status']],
                        ['#ff5d5d' if status == 'Fail' else ('#e8ece8' if status == '       DATA' else 'white') for status in combined_samples['Status']],
                        ['#ff5d5d' if status == 'Fail' else ('#e8ece8' if status == '       DATA' else 'white') for status in combined_samples['Status']]
                    ],
                    align=['left'],   
                    line_color=['black'],
                    font=dict(size=12, color=['black'])
                )
            )
        ]
    )

    # Update table layout
    table_figure.update_layout(
        margin=dict(l=15, r=5, t=50, b=0)
    )

    return table_figure

# Table with all data for all metrics and samples
def create_complete_table(data):

    data = data.sort_values("Sample ID", ascending=True)

    # Add new row to visually denote end of data
    end_of_data_row = {"Sample ID": ' ', "Gambit Predicted Taxon": '                   END', 'Number Contigs': '                    OF', 'Estimated Coverage': '                   DATA', 'Assembly Length': ' '}
    data = pd.concat([data, pd.DataFrame([end_of_data_row])], ignore_index=True)
    
    # Create the table figure
    table_figure = go.Figure(data=[go.Table(
        header=dict(
            values=[
                'Sample ID',
                'Gambit Predicted Taxon',
                'Number Contigs',
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
                data["Sample ID"],
                data["Gambit Predicted Taxon"],
                data["Number Contigs"],
                data["Estimated Coverage"],
                data["Assembly Length"],
            ],
            align='left',
            fill_color=[
                # Set background color for entire end row based to mark the end of the table
                ['#e8ece8' if status == '                   END' else 'white' for status in data["Gambit Predicted Taxon"]],
                ['#e8ece8' if status == '                   END' else 'white' for status in data["Gambit Predicted Taxon"]],
                ['#e8ece8' if status == '                   END' else 'white' for status in data["Gambit Predicted Taxon"]],
                ['#e8ece8' if status == '                   END' else 'white' for status in data["Gambit Predicted Taxon"]],
                ['#e8ece8' if status == '                   END' else 'white' for status in data["Gambit Predicted Taxon"]]
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

# Gambit breakdown per metric bar-chart
def create_gambit_breakdown_plot(data, metric_id, metric_title, thresholds):
   
    # Map organisms to their genus
    data['Genus'] = data["Gambit Predicted Taxon"].apply(map_organism_to_genus)

    # Initialize columns for 'Pass', 'Fail', and 'NA'
    data['Pass'] = 0
    data['Fail'] = 0
    data['NA'] = 0

    # Apply thresholds and set pass/fail status for each sample
    for index, row in data.iterrows():

        genus = row['Genus']
        value = row[metric_title]

        if genus in thresholds[metric_id]:

            # check threshold for assembly length with min/max values
            if metric_id == "assembly_length":
                if thresholds[metric_id][genus]['min'] <= value <= thresholds[metric_id][genus]['max']:
                    data.at[index, 'Pass'] = 1
                else:
                    data.at[index, 'Fail'] = 1
            
            # check threshold for other metrics with just one value
            else:
                threshold = thresholds[metric_id][genus]

                if ((metric_id == "est_coverage_clean" and value >= threshold) or (metric_id == "number_contigs" and value <= threshold)):
                    data.at[index, 'Pass'] = 1
                else:
                    data.at[index, 'Fail'] = 1

        # where no threshold is available, mark pass/fail status as NA            
        else:
            data.at[index, 'NA'] = 1

    # Group by Genus and sum up 'Pass', 'Fail', and 'NA'
    breakdown = data.groupby("Gambit Predicted Taxon").agg({'Pass': 'sum', 'Fail': 'sum', 'NA': 'sum'}).reset_index()

    # sort table by gambit predicted taxon
    breakdown = breakdown.sort_values("Gambit Predicted Taxon", ascending=False)

    # Create the stacked bar chart with horizontal bars
    fig = go.Figure()

    fig.add_trace(go.Bar(
        y=breakdown["Gambit Predicted Taxon"],
        x=breakdown['Pass'],
        name='Pass',
        marker_color='#67e087',
        orientation='h'
    ))

    fig.add_trace(go.Bar(
        y=breakdown["Gambit Predicted Taxon"],
        x=breakdown['Fail'],
        name='Fail',
        marker_color='#ff5c5c',
        orientation='h'
    ))

    fig.add_trace(go.Bar(
        y=breakdown["Gambit Predicted Taxon"],
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

# Table with gambit breakdown per metric
def create_gambit_breakdown_table(data, metric_id, metric_title, thresholds):

    # Initialize columns for 'Pass', 'Fail', and 'NA'
    data['Pass'] = 0
    data['Fail'] = 0
    data['NA'] = 0

    # Apply thresholds and set statuses
    for index, row in data.iterrows():

        organism = row["Gambit Predicted Taxon"]
        value = row[metric_title]
        genus = map_organism_to_genus(organism)

        if genus in thresholds[metric_id]:
            if metric_id == "assembly_length":
                # For assembly_length, check if within the min-max range
                if value >= thresholds[metric_id][genus]['min'] and value <= thresholds[metric_id][genus]['max']:
                    data.at[index, 'Pass'] = 1
                else:
                    data.at[index, 'Fail'] = 1
            else:
                # For other metrics, compare against a single threshold value
                threshold = thresholds[metric_id][genus]

                if (metric_id == "est_coverage_clean" and value >= threshold) or (metric_id == "number_contigs" and value <= threshold):
                    data.at[index, 'Pass'] = 1
                else:
                    data.at[index, 'Fail'] = 1
        else:
            # Mark as 'NA' if there's no threshold data for the genus
            data.at[index, 'NA'] = 1

    # Group by organism and calculate counts and percentages
    breakdown = data.groupby("Gambit Predicted Taxon").agg(
        Total_Count=("Gambit Predicted Taxon", 'count'),
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
                breakdown['Gambit Predicted Taxon'],
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

# define CSS style of HTML output
def HTML_head():

    html_head = '''
        <head>
            <link href="https://fonts.googleapis.com/css2?family=Droid+Sans&display=swap" rel="stylesheet">
            <meta charset="UTF-8">
            <title>Sequencing QC Report</title>
            <style>
                body {
                    font-family: 'Arial', sans-serif;
                    display: flex;
                    flex-wrap: nowrap;
                }
                #toc-title {
                    font-family: 'Droid Sans', sans-serif;
                    text-align: center;
                    padding-top: 25px;
                    padding-bottom: 10px;
                    font-size: 24px;
                    line-height: 1.2;
                    background-color: #f8f9fa;
                    width: 100%;
                    box-sizing: border-box;
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
                    width: 100%;
                    box-sizing: border-box;
                }
                #toc-container {
                    width: 250px;
                    position: fixed;
                    height: 100%;
                    overflow-y: auto;
                    background-color: #f8f9fa; 
                    padding-right: 13px;
                    padding-left: 3px;
                    transition: width 0.5s;
                    top: 0px;
                }
                ul#toc {
                    list-style-type: none;
                    padding: 0;
                    margin: 0;
                    color: #717afc;
                    font-size: 15px;
                }
                ul#toc > li > a {
                    margin-bottom: 20px;
                    margin-top: 5px;
                }
                ul#toc li a {
                    color: #8193a7;
                    text-decoration: none;
                    display: block;
                    padding: 5px 10px;
                    font-size: 14px;
                }
                ul#toc li ul li a {
                    margin-bottom: 3px;
                }
                ul#toc li ul li:last-child a {
                    margin-bottom: 7px;
                }
                ul#toc li ul li:first-child a {
                    margin-top: 5px;
                }
                ul#toc li a:hover {
                    background-color: #E2E2E2;
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
                    content: '\\25C2';
                    font-weight: bold;
                    float: right;
                    font-family: 'Arial', sans-serif;
                }
                .active:after {
                    content: '\\25BE';
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
                    margin-bottom: 0px;
                }
                .plot-table-container {
                    display: flex;
                    flex-direction: row; 
                    justify-content: space-between;
                    align-items: flex-start;
                    margin-bottom: 20px;
                    gap: 0px;
                    padding: 0px;
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
    '''

    return html_head

# retrieve data and time of workflow submission
def get_date():

    # Get current time in UTC
    current_time_utc = datetime.now(pytz.utc)

    # Format the time string in ISO 8601 format and convert to string
    formatted_time = current_time_utc.strftime('at %H:%M:%S UTC')
    formatted_date = current_time_utc.strftime('on %Y-%m-%d')

    return formatted_date, formatted_time

# define javascript functionality of HTML output
def HTML_javascript():

    javascript = '''
        <script>
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
    '''

    return javascript

# Piecing together all visualizations into HTML string
def create_HTML(data, metric_info, default_thresholds):

    # Initialize HTML 
    html_string = '''
    <!DOCTYPE html>
    <html>
    '''

    # HTML CSS/head components
    html_string += HTML_head()
    
    # Extract date and time
    date, time = get_date()

    # Intitialize HTML body section
    html_string += '''
        <body>
    '''
    
    # Initialize table of contents container and header
    html_string += f'''
            <div id="toc-container">
                <div id="toc-title">
                    Sequencing
                    <br>
                    QC Report
                </div>
                <div id="toc-info">
                    Produced with Terra 
                    <br>
                    {date}
                    <br>
                    {time}
                </div>
    '''

    # Create a list of all of the pathogens included in the samples
    organisms_in_data = sorted(set(data["Gambit Predicted Taxon"]))

    # Start table of contents section
    html_string += '''
                <ul id="toc">
    '''

    # Add table of contents elements according to data
    for metric_id, metric_title in metric_info.items():

        # Add collapsable TOC header split by metric + TOC link to plots for all samples per metric
        html_string += f'''
                    <li>
                        <button class="collapsible active">{metric_title}</button>
                        <div class="content">
                            <ul>
                                <li>
                                    <a href="#{metric_id}_total">Total</a>
                                </li>
        '''

        # Add TOC link to plots for specific pathogens per metric
        for organism in organisms_in_data:
            if is_organism_in_thresholds(organism, default_thresholds): # Only pathogens with thresholds are plotted, so only pathogens with thresholds are in TOC
                html_string += f'''
                                <li>
                                    <a href="#{metric_id}_{organism.replace(" ", "_")}">{organism}</a>
                                </li>
                '''

        # Close 
        html_string += '''
                            </ul>
                        </div>
                    </li>
        '''

    # Add link to table with all samples and close TOC section
    html_string += '''
                    <li>
                        <a href="#all_samples_table">Sample Table</a>
                    </li>
                </ul>
    '''
    
    # Close TOC container
    html_string += '''
            </div>
    '''

    # Content-container for all plots and tables
    html_string += '''
            <div id="content-container">
    '''

    # Parse through each metric to create metric-specific plots
    for metric_id, metric_title in metric_info.items():

        # Metric plot for all samples agnostic of pathogen (total per metric)
        html_string += f'''
                <div id="{metric_id}_total">
                    <h2>{metric_title} per sample</h2>
        '''

        # Create total metric plot 
        fig = create_scatter_plot(data, metric_title)
        html_string += fig.to_html(full_html=False, include_plotlyjs='cdn', config={'responsive': True})

        # End of total metric section
        html_string += '''
                </div>'''

        # Start of gambit breakdown section
        html_string += '''
                <div id="gambit_breakdown" class="section-container">
                    <div class="plot-table-container">
        '''

        # Start gambit breakdown plot
        html_string += '''
                        <div class="gambit-container">
        '''
        # Add the gambit breakdown plot
        plot_fig = create_gambit_breakdown_plot(data, metric_id, metric_title, default_thresholds)
        html_string += plot_fig.to_html(full_html=False, include_plotlyjs='cdn', config={'responsive': True})

        # End of gambit breakdown plot and start of gambit breakdown table
        html_string += '''
                        </div>
        '''

        # Start gambit breakdown table
        html_string += '''
                        <div class="table-container">
        '''

        # Add the gambit breakdown table
        table_fig = create_gambit_breakdown_table(data, metric_id, metric_title, default_thresholds)
        html_string += table_fig.to_html(full_html=False, include_plotlyjs='cdn')

        # End of table container
        html_string += '''
                        </div>
        '''

        # End of plot-table container and end of gambit breakdown section
        html_string += '''
                    </div>
                </div>
        '''

        # Parse through pathogens to create pathogen-specific metric tables
        for organism in organisms_in_data:

            if is_organism_in_thresholds(organism, default_thresholds): # Only pathogens with thresholds are plotted

                genus = map_organism_to_genus(organism) # thresholds are by genus, so sort each pathogen into its respective genus
                threshold = default_thresholds[metric_id].get(genus) # extract threshold data according to genus
            
                # Start section container for pathogen specific plot and table
                html_string += f'''
                <div id="{metric_id}_{organism.replace(" ", "_")}" class="section-container">
                '''

                # Title for section
                html_string += f'''
                    <div class="section-title">
                        <h3>{metric_title} per sample for {organism}</h3>
                    </div>
                '''

                # Logic for threshold informational data
                if metric_id == "est_coverage_clean": 
                    html_string += f'''
                    <div class="threshold-info">
                        Threshold for {genus}: Values greater than {threshold} are considered passing. Any values less than {threshold} are considered failures.
                    </div>'''
                
                elif metric_id == "number_contigs":
                    html_string += f'''
                    <div class="threshold-info">
                        Threshold for {genus}: Values less than {threshold} are considered passing. Any values greater than {threshold} are considered failures.
                    </div>'''
                
                else:
                    min_thres = threshold.get("min")
                    max_thres = threshold.get("max")
                    html_string += f'''
                    <div class="threshold-info">
                        Thresholds for {genus}: Minimum {min_thres}, Maximum {max_thres}. Values outside this range are considered failures.
                    </div>'''

                # Start the container for plot and table
                html_string += '''
                    <div class="plot-table-container">
                '''
                
                # Add the plot to its own container
                html_string += '''
                        <div class="plot-container">
                '''
                
                # Create pathogen-specific plot for specific metric
                fig = create_organism_specific_plot(data, metric_title, organism, threshold)
                html_string += fig.to_html(full_html=False, include_plotlyjs='cdn', config={'responsive': True})
                
                # End of plot container
                html_string += '''
                        </div>
                '''
                
                # Add the table to its own container
                html_string += '''
                        <div class="table-container">
                '''

                # Create table with all samples of same pathogen for specific metric
                table_fig = create_organism_specific_table(data, metric_title, organism, threshold)
                html_string += table_fig.to_html(full_html=False, include_plotlyjs='cdn')

                # End of table container
                html_string += '''
                        </div>
                '''

                # Close the container for the plot and the table
                html_string += '''
                    </div>
                '''  

                # Close the section container
                html_string += '''
                </div>
                '''

    # Content for datatable containing all samples
    html_string += f'''
                <div id="all_samples_table">
                    <h2>Sample Table</h2>
    '''
    #Create table with all samples
    complete_table_fig = create_complete_table(data)
    html_string += complete_table_fig.to_html(full_html=False, include_plotlyjs='cdn')

    # End of table container
    html_string += '''
                </div>
    '''
            
    # End of content container
    html_string += '''
            </div>
    '''

    # Add javascript functionality
    html_string += HTML_javascript()

    # Close HTML body
    html_string += '''
        </body>
    '''

    # Close HTML string
    html_string += '''
    </html>
    '''

    return html_string

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="create visualizations")

    parser.add_argument("-s", "--samples", required=True, type=str, nargs="+", help="List of entity sample-id values")
    parser.add_argument("-g", "--gambit_predicted_taxon", required=True, type=str, nargs="+", help="List of gambit predicted taxons for relevant ids")
    parser.add_argument("-ecc", "--est_coverage_clean", required=True, type=float, nargs="+", help="List of estimated coverages for relevant ids")
    parser.add_argument("-nc", "--number_contigs", required=True, type=int, nargs="+", help="List of number contigs for relevant ids")
    parser.add_argument("-al", "--assembly_length", required=True, type=int, nargs="+", help="List of assembly lengths for relevant ids")
    parser.add_argument("-o", "--output_filename", required=True, default="QC_visualizations.html", help="Name of output HTML file containing visualizations")
    parser.add_argument("-t", "--thresholds_file", type=str, help="Path to a JSON file containing custom thresholds")

    args = parser.parse_args()

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

    # Map metric identifiers with formatted metric titles
    metric_info = {"est_coverage_clean": "Estimated Coverage", "number_contigs": "Number Contigs", "assembly_length": "Assembly Length"}

    # Override default thresholds
    if args.thresholds_file:
        update_thresholds_from_file(args.thresholds_file, default_thresholds)

    # extracting data from inputs and formatting into dataframe
    try:
        data = pd.DataFrame({
            'Sample ID': args.samples,
            'Gambit Predicted Taxon': args.gambit_predicted_taxon,
            'Estimated Coverage': args.est_coverage_clean,
            'Number Contigs': args.number_contigs,
            'Assembly Length': args.assembly_length
        })  
    # throw custom error message if data is missing
    except ValueError:
        print("Error: Selected data does not have all estimated coverage, number contigs, assembly length, and/or predicted gambit values filled in for all rows. Make sure your selected data has all the required values filled in for all rows.")
        exit(1)

    # create HTML string and all visuals
    html_string = create_HTML(data, metric_info, default_thresholds)

    # Write the combined HTML string to a file with UTF-8 encoding
    with open(args.output_filename, "w", encoding='utf-8') as file:
        file.write(html_string)