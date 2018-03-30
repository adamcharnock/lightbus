"""
This web server does not access the bus at all. It simply
reads data from the .exampledb.json json file created by
bus.py
"""

import json
from flask import Flask

app = Flask(__name__)

@app.route('/')
def home():
    html = '<h1>Dashboard</h1>\n'
    html = '<p>Total store views</p>\n'

    with open('/tmp/.exampledb.json', 'r') as f:
        page_views = json.load(f)

    html += '<ul>'
    for url, total_views in page_views.items():
        html += f'<li>URL <code>{url}</code>: {total_views}</li>'
    html += '</ul>'

    return html
