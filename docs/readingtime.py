""" Reading time for sphinx

Credit to: https://github.com/hamukazu/wordcount-sphinx
Adapted by Adam Charnock
"""
from docutils import nodes


def setup(app):
    app.add_node(ReadingTime)
    app.add_directive('readingtime', ReadingTimeDirective)
    app.connect('doctree-resolved', process_readingtime_nodes)


class ReadingTime(nodes.General, nodes.Element):
    pass


from sphinx.util.compat import Directive


class ReadingTimeDirective(Directive):
    has_content = True

    def run(self):
        return [ReadingTime('')]


def process_readingtime_nodes(app, doctree, fromdocname):
    env = app.builder.env
    count = 0
    for node in doctree.traverse(nodes.paragraph):
        tt=node.astext()
        count+=len(tt.split(" "))

    for node in doctree.traverse(ReadingTime):
        para = nodes.rubric()
        minutes = int(round(count / 200.0))
        para += nodes.Text("Reading time: {} {}".format(
            minutes,
            'minute' if minutes == 1 else 'minutes'
        ))
        node.replace_self([para])
        
        

