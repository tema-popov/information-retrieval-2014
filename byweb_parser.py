from StringIO import StringIO
import base64
import logging
from BeautifulSoup import BeautifulSoup
from lxml import etree, html

def clean_html(text):
    # Encoding + entities
    try:
        soup = BeautifulSoup(text, convertEntities=BeautifulSoup.HTML_ENTITIES)
        body = soup.body
        if body:
            soup = body
        for script in soup(("script", "style")):
            script.extract()
        return soup.getText(" ")
    except:
        logging.error(text)
        logging.exception("cleaning html exception")
        return text

def base64_decode(coded_string):
    return base64.b64decode(coded_string)

class ClassChainWrapper(object):
    def __init__(self, *files):
        self.files = files
        self.i = 0

    def read(self, n):
        read = self.files[self.i].read(n).replace('&', '&amp;')
        if read:
            return read
        else:
            if self.i == len(self.files) - 1:
                return read
            else:
                self.i += 1
                return self.read(n)


def iterate_documents(xml_file, file_name):
    try:
        # all by_web xmls except first doesn't have root element
        source = ClassChainWrapper(StringIO("<root>\n"), xml_file, StringIO("\n</root>"))
        xml_file_iterator = etree.iterparse(source, events=("end", ), huge_tree=True, recover=True, encoding="cp1251")
        doc_id, doc_url, doc_text = None, None, None
        for event, element in xml_file_iterator:
            if element.tag == 'docID':
                doc_id = element.text
            elif element.tag == 'docURL':
                try:
                    doc_url = element.text
                except:
                    logging.exception("doc_url_exc")
                    doc_url = "example.com"
            elif element.tag == 'content':
                try:
                    doc_text = base64_decode(element.text)
                except:
                    logging.exception("doc_text_exc")
                    doc_text = ""
            elif element.tag == 'document':
                element.clear()
                yield doc_id, doc_url, doc_text
    except:
        logging.exception(file_name)
