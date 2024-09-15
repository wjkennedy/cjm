from flask import Flask, render_template, request, redirect, url_for
import sqlite3
import json
import os
import networkx as nx
import plotly
import plotly.graph_objs as go

app = Flask(__name__)

# Database connection function
def get_db_connection():
    conn = sqlite3.connect('database/customer_journey.db')
    conn.row_factory = sqlite3.Row
    return conn

# Function to create tables if they don't exist
def initialize_database():
    conn = get_db_connection()
    cursor = conn.cursor()
    # Create Customers table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Customers (
            customer_id TEXT PRIMARY KEY,
            version TEXT NOT NULL
        );
    ''')
    # Create Journey_Steps table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Journey_Steps (
            step_id TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL,
            step_name TEXT NOT NULL,
            timestamp DATETIME NOT NULL,
            contact_method TEXT NOT NULL,
            lead_time REAL,
            handoff_to TEXT,
            version TEXT NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES Customers(customer_id)
        );
    ''')
    conn.commit()
    conn.close()

# Function to insert customer
def insert_customer(cursor, customer_id, version):
    cursor.execute('''
        INSERT OR IGNORE INTO Customers (customer_id, version) VALUES (?, ?)
    ''', (customer_id, version))

# Function to insert journey step
def insert_journey_step(cursor, step):
    cursor.execute('''
        INSERT OR REPLACE INTO Journey_Steps (
            step_id, customer_id, step_name, timestamp,
            contact_method, lead_time, handoff_to, version
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        step['step_id'],
        step['customer_id'],
        step['step_name'],
        step['timestamp'],
        step['contact_method'],
        step.get('lead_time'),
        step.get('handoff_to'),
        step['version']
    ))

# Function to load JSON data into the database
def load_json_to_db(cursor, json_data):
    version = json_data['version']
    for customer in json_data['customers']:
        customer_id = customer['customer_id']
        insert_customer(cursor, customer_id, version)
        for step in customer['journey']:
            step['customer_id'] = customer_id
            step['version'] = version
            insert_journey_step(cursor, step)

# Function to get customer journey from the database
def get_customer_journey(cursor, customer_id, version):
    cursor.execute('''
        SELECT * FROM Journey_Steps
        WHERE customer_id = ? AND version = ?
        ORDER BY timestamp
    ''', (customer_id, version))
    return cursor.fetchall()

# Function to create the DAG
def create_customer_journey_dag(journey_steps):
    G = nx.DiGraph()
    for step in journey_steps:
        G.add_node(step['step_id'], label=step['step_name'])
        if step['handoff_to']:
            G.add_edge(step['step_id'], step['handoff_to'], lead_time=step['lead_time'])
    return G

# Function to plot the graph using Plotly
def plot_graph(G):
    pos = nx.spring_layout(G)
    edge_trace = []
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_trace.append(go.Scatter(
            x=[x0, x1, None],
            y=[y0, y1, None],
            line=dict(width=1, color='#888'),
            hoverinfo='none',
            mode='lines'
        ))
    node_trace = go.Scatter(
        x=[pos[node][0] for node in G.nodes()],
        y=[pos[node][1] for node in G.nodes()],
        text=[G.nodes[node]['label'] for node in G.nodes()],
        mode='markers+text',
        textposition='top center',
        hoverinfo='text',
        marker=dict(
            showscale=False,
            color='lightblue',
            size=20,
            line=dict(width=2)
        )
    )
    layout = go.Layout(
        title='Customer Journey Map',
        showlegend=False,
        hovermode='closest',
        margin=dict(b=20, l=5, r=5, t=40),
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)
    )
    fig = go.Figure(data=edge_trace + [node_trace], layout=layout)
    graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    return graphJSON

# Home page route
@app.route('/')
def index():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT DISTINCT customer_id FROM Journey_Steps')
    customers = [row['customer_id'] for row in cursor.fetchall()]
    conn.close()
    return render_template('index.html', customers=customers)

# Upload JSON file route
@app.route('/upload', methods=['GET', 'POST'])
def upload():
    if request.method == 'POST':
        json_file = request.files['json_file']
        if json_file:
            filename = json_file.filename
            filepath = os.path.join('data/uploaded_json_files', filename)
            json_file.save(filepath)
            with open(filepath, 'r') as f:
                json_data = json.load(f)
            # Load data into the database
            conn = get_db_connection()
            cursor = conn.cursor()
            load_json_to_db(cursor, json_data)
            conn.commit()
            conn.close()
            return redirect(url_for('index'))
    return render_template('upload.html')

# Visualization route
@app.route('/visualize/<customer_id>')
def visualize(customer_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    journey_steps = get_customer_journey(cursor, customer_id, version='1.0')
    # Convert to list of dicts
    journey_steps = [
        {
            'step_id': row['step_id'],
            'step_name': row['step_name'],
            'timestamp': row['timestamp'],
            'contact_method': row['contact_method'],
            'lead_time': row['lead_time'],
            'handoff_to': row['handoff_to']
        } for row in journey_steps
    ]
    conn.close()
    # Create the graph
    G = create_customer_journey_dag(journey_steps)
    graphJSON = plot_graph(G)
    return render_template('visualize.html', graphJSON=graphJSON, customer_id=customer_id)

if __name__ == '__main__':
    initialize_database()
    app.run(debug=True)
