#!/usr/bin/env python
# encoding: utf-8
"""matplotlib_example.py
   An simple example of how to insert matplotlib generated figures
   into a ReportLab platypus document.
"""

import matplotlib
matplotlib.use('PDF')
import matplotlib.pyplot as plt
import cStringIO

from pdfrw import PdfReader
from pdfrw.buildxobj import pagexobj
from pdfrw.toreportlab import makerl

from reportlab.platypus import Flowable
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.rl_config import defaultPageSize
from reportlab.lib.units import inch

PAGE_HEIGHT = defaultPageSize[1]; PAGE_WIDTH=defaultPageSize[0]
styles = getSampleStyleSheet()


class PdfImage(Flowable):
    """PdfImage wraps the first page from a PDF file as a Flowable
which can be included into a ReportLab Platypus document.
Based on the vectorpdf extension in rst2pdf (http://code.google.com/p/rst2pdf/)"""

    def __init__(self, filename_or_object, width=None, height=None, kind='direct'):
        from reportlab.lib.units import inch
        # If using StringIO buffer, set pointer to begining
        if hasattr(filename_or_object, 'read'):
            filename_or_object.seek(0)
        page = PdfReader(filename_or_object, decompress=False).pages[0]
        self.xobj = pagexobj(page)
        self.imageWidth = width
        self.imageHeight = height
        x1, y1, x2, y2 = self.xobj.BBox

        self._w, self._h = x2 - x1, y2 - y1
        if not self.imageWidth:
            self.imageWidth = self._w
        if not self.imageHeight:
            self.imageHeight = self._h
        self.__ratio = float(self.imageWidth)/self.imageHeight
        if kind in ['direct', 'absolute'] or width==None or height==None:
            self.drawWidth = width or self.imageWidth
            self.drawHeight = height or self.imageHeight
        elif kind in ['bound', 'proportional']:
            factor = min(float(width)/self._w, float(height)/self._h)
            self.drawWidth = self._w*factor
            self.drawHeight = self._h*factor

    def wrap(self, aW, aH):
        return self.drawWidth, self.drawHeight

    def drawOn(self, canv, x, y, _sW=0):
        if _sW > 0 and hasattr(self, 'hAlign'):
            a = self.hAlign
            if a in ('CENTER', 'CENTRE', TA_CENTER):
                x += 0.5*_sW
            elif a in ('RIGHT', TA_RIGHT):
                x += _sW
            elif a not in ('LEFT', TA_LEFT):
                raise ValueError("Bad hAlign value " + str(a))

        xobj = self.xobj
        xobj_name = makerl(canv._doc, xobj)

        xscale = self.drawWidth/self._w
        yscale = self.drawHeight/self._h

        x -= xobj.BBox[0] * xscale
        y -= xobj.BBox[1] * yscale

        canv.saveState()
        canv.translate(x, y)
        canv.scale(xscale, yscale)
        canv.doForm(xobj_name)
        canv.restoreState()

Title = "Hello world"
pageinfo = "platypus example"


def myFirstPage(canvas, doc):
    canvas.saveState()
    canvas.setFont('Times-Bold',16)
    canvas.drawCentredString(PAGE_WIDTH/2.0, PAGE_HEIGHT-108, Title)
    canvas.setFont('Times-Roman',9)
    canvas.drawString(inch, 0.75 * inch, "First Page / %s" % pageinfo)
    canvas.restoreState()


def myLaterPages(canvas, doc):
    canvas.saveState()
    canvas.setFont('Times-Roman',9)
    canvas.drawString(inch, 0.75 * inch, "Page %d %s" % (doc.page, pageinfo))
    canvas.restoreState()

def go():
    fig = plt.figure(figsize=(4, 3))
    plt.plot([1, 2, 3, 4])
    plt.ylabel('some numbers')
    imgdata = cStringIO.StringIO()
    fig.savefig(imgdata, format='PDF')
    doc = SimpleDocTemplate("document.pdf")
    Story = [Spacer(1, 2*inch)]
    style = styles["Normal"]
    for i in range(5):
        bogustext = ("This is Paragraph number %s.  " % i) * 20
        p = Paragraph(bogustext, style)
        Story.append(p)
        Story.append(Spacer(1, 0.2*inch))
        pi = PdfImage(imgdata)
        Story.append(pi)
        Story.append(Spacer(1, 0.2*inch))
    doc.build(Story, onFirstPage=myFirstPage, onLaterPages=myLaterPages)


if __name__ == '__main__':
    go()
