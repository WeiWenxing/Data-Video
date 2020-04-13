# -*- coding: utf-8 -*-

import urllib
import urllib.request


def download(url, outfile):
    print("downloading with urllib: " + url)
    urllib.request.urlretrieve(url, outfile)
    print("downloaded success: " + url)
    return


area = 'https://github.com/BlankerL/DXY-COVID-19-Data/raw/master/csv/DXYArea.csv'
download(area, 'DXYArea.csv')
overall = 'https://github.com/BlankerL/DXY-COVID-19-Data/raw/master/csv/DXYOverall.csv'
download(overall, 'DXYOverall.csv')
