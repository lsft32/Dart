from PyPDF3 import PdfFileMerger, PdfFileReader
import os, glob

path = "C:/cloud2/company"
filename = glob.glob(path + '/*.pdf')

if not filename:
    print('Put your fucking PDF')
else:
    merger = PdfFileMerger()
    for filenames in filename:
        merger.append(PdfFileReader(open(filenames, 'rb')))
        print(filename)

merger.write(path + '/입사지원서(정율권).pdf')
print('Complete!!')

