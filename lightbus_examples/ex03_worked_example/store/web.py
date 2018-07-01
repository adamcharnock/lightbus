"""A simple pet shop

Shows a list of animals, and you can click on each one.

Image resizing and page view tracking performed using lightbus.
"""
import lightbus
from flask import Flask

from lightbus_examples.ex03_worked_example.store.bus import bus

app = Flask(__name__)

lightbus.configure_logging()

PETS = (
    "http://store.company.com/image1.jpg",
    "http://store.company.com/image2.jpg",
    "http://store.company.com/image3.jpg",
)


@app.route("/")
def home():
    html = "<h1>Online pet store</h1><br>"

    for pet_num, image_url in enumerate(PETS):
        resized_url = bus.image.resize(url=image_url, width=200, height=200)
        html += f'<a href="/pet/{pet_num}">' f'<img src="{resized_url}">' f"</a> "

    bus.store.page_view.fire(url="/")
    return html


@app.route("/pet/<int:pet_num>")
def pet(pet_num):
    resized_url = bus.image.resize(url=PETS[pet_num], width=200, height=200)
    bus.store.page_view.fire(url=f"/pet/{pet_num}")

    html = f"<h1>Pet {pet_num}</h1>"
    html = f'<img src="{resized_url}"><br />'
    return html
