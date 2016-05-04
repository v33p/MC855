#! C:\python27

import re, urllib

def getDecendentsUrl (url, result):
	if (url.startswith(myurl) or url.startswith('/')):
		if (url.startswith('/')):
			url = myurl + url[1:]
		finded = False
		print url
		for r in result:
			if (r[1] == url):
				finded = True
				r[0] = r[0] + 1
		if (finded == False):
			t = []
			count = 1
			t.append(count)
			t.append(url)
			result.append(t)
			for i in re.findall('''href=["'](.[^"']+)["']''', urllib.urlopen(url).read(), re.I):
				getDecendentsUrl(i, result)	
		
def getCount(item):
	return item[0]

textfile = file('depth_1.txt','wt')
result = []
print "Enter the URL you wish to crawl.."
print 'Usage  - "http://phocks.org/stumble/creepy/" <-- With the double quotes'
myurl = input("@> ")
getDecendentsUrl(myurl, result)
for t in sorted(result, key=getCount):
	textfile.write(str(t[0]) + " " + t[1] + "\n")
textfile.close()
