#! C:\python27

import re, urllib

def getDecendentsUrl (url, textfile):
	if (url.startswith(myurl)):
		print url
		textfile.write(url)
		for i in re.findall('''href=["'](.[^"']+)["']''', urllib.urlopen(myurl).read(), re.I):
			getDecendentsUrl(i, textfile)	

textfile = file('depth_1.txt','wt')
print "Enter the URL you wish to crawl.."
print 'Usage  - "http://phocks.org/stumble/creepy/" <-- With the double quotes'
myurl = input("@> ")
getDecendentsUrl(myurl, textfile)
textfile.close()
