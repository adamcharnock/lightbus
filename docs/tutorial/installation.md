
## 1.1. Preface: Installing Python 3.7 (or above)

Lightbus requires Python 3.7 or newer. This is available
for all major operating systems.

### Python 3.7 on macOS

You can check your current version of Python as follows:

    $ python3 --version
    Python 3.7.2

You need version 3.7 or above to run Lightbus.

If you are running an older version of Python you can install a newer
version via one of the following methods:

1. [Install Python 3.7 using Homebrew][vincent] – This is the easiest option, you will
   install that latest version of Python 3.
2. [Install Python 3.7 using Homebrew + pyenv][gardner] – This option has some additional
   steps, but you will have complete control over the Python versions available to you.
   If you work on multiple Python projects this may be more suitable.
3. [Install Python 3.7 manually][download] – Not recommended

### Python 3.7 on Linux

Your Linux distribution may already come with Python 3.7 installed. You can check your
Python version as follows:

    $ python3 --version
    Python 3.7.2

You need version 3.7 or above to run Lightbus. Digital Ocean has a
[beginner-suitable guide][digital-ocean] on installing Python 3 which you may find useful.

If you require more granular control of your python versions you may find [pyenv] more suitable.

### Windows

Lightbus is not currently tested for deployment on Windows, so your millage may vary.
The Hitchhiker's Guide to Python covers [installing Python 3 on Windows][god-help-you].

## 1.2. Installing Lightbus

### Installing using pip (recommended)

**At time of writing we were yet to have an official release. Please install via git in the mean-time.**

    $ pip3 install lightbus

### Installing using git

This will clone the bleeding-edge version Lightbus and install it ready to use. This is useful
if you need the latest (albeit unstable) changes, or if you wish to modify the Lightbus source.

    $ pip install https://github.com/adamcharnock/lightbus.git#egg=lightbus

## 1.3. Installing Redis

You will need Redis 5.0 or above in order to use Lightbus.

You can install Redis 5.0 on macOS by either:

1. Using [Homebrew] (`brew install redis`), or
2. Using docker (`docker run --rm -p 6379:6379 -d redis`) 

## 1.4. Check it works

You should now have:

1. Python 3.7 or above installed
2. Lightbus installed
3. Redis installed and running

You check check everything is setup correctly by starting up lightbus. First create a file 
name `bus.py` which contains:

```python
# Create this in a bus.py file
import lightbus

bus = lightbus.create()
```

Now run the command:

    $ lightbus run

Lightbus should start without errors and wait for messages.
You can exit using ++ctrl+c++.

This Lightbus process isn't setup to do anything, but its a start!

## 1.5 Next

Now you are up and running, take a look at the [quick start tutorial].

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
[quick start tutorial]: quick-start.md
