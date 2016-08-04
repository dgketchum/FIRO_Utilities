
from mechanize import Browser, HTTPError, URLError
import contextlib
from BeautifulSoup import BeautifulSoup
from time import sleep

req = 'http://waterdata.usgs.gov/nwis/uv?site_no=11461000'
br = Browser()
br.open(req)
try_ct = 0
try:
    with contextlib.closing(br.open(req)) as page:
        print 'opened'
        soup = BeautifulSoup(page.read())
        input_divs = soup.findAll('div')
        for div in input_divs:
            print 'type of div: {}'.format(type(div))
            if div['class'] == 'available_parameters_float_2'
                for input in div:
                    if input['id'] ==
        print soup
except (HTTPError, URLError) as e:
    try_ct += 1
    if isinstance(e, HTTPError):
        print e.code
    else:
        print e.reason.args
    if try_ct > 4:
        exit()
    sleep(30)


def select_form(form):
    return form.attrs.get('id', None) == 'available_parameters_form'
br.select_form(predicate=select_form)
for control in br.form.controls:
    print control
    print 'type = {} name = {} '.format(control.type, control.name)
    if control.name == 'cb_00060':
        q_cb = control.name
    elif control.name == 'cb_00065':
        stg_cb = control.name

