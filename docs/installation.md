
## Preface: Installing Python 3.6 (or above)

Lightbus requires Python 3.6 or newer as it relies upon Python's new 
[asyncio] and [type hinting] features. This is readily available 
for all major operating systems but you should make sure you have it 
installed before continuing.

### macOS

You can check your current version of Python as follows:

    $ python3 --version
    Python 3.6.4

You need version 3.6 or above to run Lightbus. You can install it
by choosing one of the following:

1. [Install Python 3.6 using Homebrew][vincent] – This is the easiest option, you will
   install that latest version of Python 3.
2. [Install Python 3.6 using Homebrew + pyenv][gardner] – This option has some additional
   steps, but you will have complete control over the Python versions available to you.
   If you work on multiple Python projects this may be more suitable.
3. [Install Python 3.6 manually][download] – Not recommended

### Linux

Your Linux distribution may already come with Python 3.6 installed. You can check your 
Python version as follows:

    $ python3 --version
    Python 3.6.4

You need version 3.6 or above to run Lightbus. Digital Ocean has a 
[beginner-suitable guide][digital-ocean] on installing Python 3 which you may find useful.

If you require more granular control of your python versions you may find [pyenv] more suitable.

### Windows

Lightbus is not currently tested for deployment on Windows, so your millage may vary. 
The Hitchhiker's Guide to Python covers [installing Python 3 on Windows][god-help-you].

## Installing Lightbus

### Installing using pip (recommended)

*At time of writing we were yet to do an official release. Please install via git in the mean-time.*

    $ pip3 install lightbus

### Installing using git

This will clone the bleeding-edge version Lightbus and install it ready to use. This is useful 
if you need the latest (albeit unstable) changes, or if you wish to modify the Lightbus source.

    $ cd my-project/
    
    $ mkdir vendor
    $ git clone https://github.com/adamcharnock/lightbus.git vendor/lightbus
    $ pip3 install -e vendor/lightbus

## Installing Redis

Lightbus relies on [Redis] as its primary transport. Specifically it relies on the new Redis 'streams' 
feature to provide robust message handling. This will be available in Redis 5.0, due around mid-2018.
Until then you should build Redis from source as follows:

    $ git clone https://github.com/antirez/redis.git
    $ cd redis
    $ make
    $ ./src/redis-server

## Check it works

You should now have:

1. Python 3.6 or above installed
2. Lightbus installed
3. Redis installed and running

You check check everything is setup correctly by starting up lightbus:

    $ lightbus run

# TODO: Doesn't work

[vincent]: https://wsvincent.com/install-python3-mac/
[gardner]: https://medium.com/@jordanthomasg/python-development-on-macos-with-pyenv-2509c694a808
[Homebrew]: https://brew.sh/
[pyenv]: https://github.com/pyenv/pyenv
[download]: https://www.python.org/downloads/mac-osx/
[digital-ocean]: https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-local-programming-environment-on-ubuntu-16-04
[god-help-you]: http://docs.python-guide.org/en/latest/starting/install3/win/
[Redis]: https://redis.io/
<!-- Seriously, the Python docs for asyncio are scary. Let's link to something nicer -->
[asyncio]: https://hackernoon.com/asyncio-for-the-working-python-developer-5c468e6e2e8e
[type hinting]: https://docs.python.org/3/library/typing.html
