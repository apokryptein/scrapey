from urllib.request import urlopen, Request
from urllib.error import HTTPError
from urllib.error import URLError
from urllib.parse import urlparse
import urllib
from bs4 import BeautifulSoup
import re
import sys
import optparse
from pymysql import MySQLError
import pymysql



# Create MySQL database connection to local machineand
# Create a cursor and navigate to scraping database
# Uncomment following block for database connection
# try:
# 	conn = pymysql.connect(host = '127.0.0.1', user = 'root', passwd = '<PASSWORD>!', db = 'mysql')
# 	cursor = conn.cursor()
# 	cursor.execute("USE scraping")
# except MySQLError as e:
# 	print('Got error {!r}, errorno is {}'.format(e, e.args[0]))
# 	sys.exit()

# Create option parsing object for addition of command line options
p = optparse.OptionParser(description = 'Python Web Crawler & Scraper', prog = 'crawler', version = '0.01',
							usage = "usage: %prog [options] URL")
p.add_option('--max-depth', '-d', type='int', dest="depth", default=5, help='Maximum crawl depth.  Default: 5')
p.add_option('-s', action = 'store_true', help = 'Crawl only within scope of seed URL')


# Parse options and arguments and assign to variables
options, arguments = p.parse_args()


#Retrieves a list of all external links found on a page
def getExternalLinks(soup, excludeUrl):
    externalLinks = []
    for link in soup.findAll(["a", "link"], href=re.compile("^(http|https|www)((?!"+excludeUrl+").)*$")):
        if link.attrs['href'] is not None:
            if link.attrs['href'] not in externalLinks:
                externalLinks.append(link.attrs['href'])
    return externalLinks



#Retrieves a list of all Internal links found on a page
def getInternalLinks(soup, includeUrl):
    includeUrl = urlparse(includeUrl).scheme+"://"+urlparse(includeUrl).netloc
    internalLinks = []
    #Finds all links that begin with a "/"
    for link in soup.findAll("a", href=re.compile("^(/|.*"+includeUrl+")")):
        if link.attrs['href'] is not None:
            if link.attrs['href'] not in internalLinks:
                if(link.attrs['href'].startswith("/")):
                    internalLinks.append(includeUrl+link.attrs['href'])
                else:
                    internalLinks.append(link.attrs['href'])
    return internalLinks


# Splits URL into its parts
def splitAddress(address):
    addressParts = address.replace("http://", "").split("/")
    return addressParts


# Get HTML of URL and create BS object
def loadPage(startingSite):
	r = Request(startingSite, headers={'User-Agent': 'Mozilla/5.0'})
	try:
		html = urlopen(r)
		soup = BeautifulSoup(html, "html.parser")
		return getExternalLinks(soup, splitAddress(startingSite)[0]), getInternalLinks(soup, startingSite)
	except urllib.error.HTTPError as err:
		if err.code == 404:
			print("404: Page not found!")
		elif err.code == 403:
			print("403: Access denied!")
		elif err.code == 999:
			print("999: Request denied!")
		else:
			print("Another error occurred.")
	except urllib.error.URLError as e:
		print('Got error {!r}, errorno is {}'.format(e, e.args[0]))
		# print("Exiting...")
		# sys.exit()

# Store URL to scraping database pages table	
def storeUrl(url):
	try:
		cursor.execute("INSERT INTO pages (content) VALUES (\"%s\")", (url))
		cursor.connection.commit()
	except MySQLError as e:
		print('Got error {!r}, errorno is {}'.format(e, e.args[0]))
		f = open("alternative_output.txt", "a+")
		f.write(url + "\n")
		f.close



# Join two lists
def union(a, b):
    for e in b:
        if e not in a:
            a.append(e)

# Function crawling all internal and external links
def crawl_full_web(seed, max_depth):
    tocrawl = [seed]
    crawled = []
    graph = {}  # <url>, [list of pages it links to]
    next_depth = []
    depth = 0
    while tocrawl and depth <= max_depth: 
        page = tocrawl.pop()
        if page not in crawled:
        	if loadPage(page) is not None:
	            print(page)
	            outlinks, inlinks = loadPage(page)
	            allLinks = outlinks + inlinks
	            # Uncomment following line to store data in DB
	            # storeUrl(page)
	            graph[page] = allLinks
	            # union(tocrawl, allLinks)
	            union(next_depth, allLinks)
	            crawled.append(page)
        if not tocrawl:
            tocrawl, next_depth = next_depth, []
            depth += 1
    return graph


def crawl_scope(seed, max_depth):
    tocrawl = [seed]
    crawled = []
    graph = {}  # <url>, [list of pages it links to]
    next_depth = []
    depth = 0
    while tocrawl and depth <= max_depth: 
        page = tocrawl.pop()
        if page not in crawled:
        	if loadPage(page) is not None:
	            print(page)
	            outlinks, inlinks = loadPage(page)
	            allLinks = inlinks
	            # Uncomment following line to store data in DB
	            # storeUrl(page)
	            graph[page] = allLinks
	            # union(tocrawl, allLinks)
	            union(next_depth, allLinks)
	            crawled.append(page)
        if not tocrawl:
            tocrawl, next_depth = next_depth, []
            depth += 1
    return graph


if __name__ == '__main__':
	if len(arguments) == 1:
		try :
			url = arguments[0]
			if options.s:
				crawl_scope(url, options.depth)
			else:
				crawl_full_web(url, options.depth)
		except KeyboardInterrupt:
			print("Scan canceled by user.")
			print("Thank you for crawling.")
			# cursor.close()
			# conn.close()
			sys.exit()
	else:
		p.print_help()


