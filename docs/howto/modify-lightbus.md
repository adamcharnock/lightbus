# How to modify Lightbus

Contributions to Lightbus are very welcome. This will talk you though setting up a 
development installation of Lightbus. Using this installation you will be able to:

* Modify the Lightbus source code and/or documentation
* Run the Lightbus test suite
* View any modified documentation locally
* Use your development Lightbus install within another project 

## Prerequisites

You will need:

* Redis running locally

## Getting the code

Checkout the Lightbus repository from GitHub:

    git clone https://github.com/adamcharnock/lightbus.git
    cd lightbus

## Environment setup

It is a good idea to put `asyncio` into debug mode. You can do this by setting the following in 
your shell's environment:

    PYTHONASYNCIODEBUG=1 

The testing framework will also need to know where your redis instance is running.
This is set using the `REDIS_URL` and `REDIS_URL_B` environment variables:
    
    # Default values shown below
    REDIS_URL=redis://127.0.0.1:6379/10
    REDIS_URL_B=redis://127.0.0.1:6379/11

## Installation

You will need to install Lightbus' standard dependencies, as well as Lightbus' development 
dependencies. You can install both of these groups as follows:

    pipenv install --dev

## Running the tests

You can run the tests once you have completed the above steps:

    pytest

Note that you can run subsets of the tests as follows:

    pytest -m unit  # Fast with high coverage
    pytest -m integration
    pytest -m reliability

## Using within your project

You can install your development Lightbus install within your 
project as follows:

    # Within your own project
    
    # Make sure you remove any existing lightbus version
    pip uninstall lightbus
    
    # Install your local development lightbus
    pip install --editable /path/to/your/local/lightbus

## Viewing the Lightbus documentation locally

You can view the documentation of your local Lightbus install as follows:

    mkdocs serve 

You can now view the documentation http://127.0.0.1:8000. 
The documentation source can be found in `docs/`.

## See also

Being familiar with the [explanation](../explanation/index.md) section is highly recommended 
if modifying the Lightbus source
