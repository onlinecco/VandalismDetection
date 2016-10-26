import xml.etree.ElementTree as ET
import hashlib

tree = ET.parse('wdvc16_2013_01.xml')
root = tree.getroot()
namespace = root.tag[:root.tag.index('}')+1]
with open ('out.csv','w') as f:
	for p in root.iter(namespace+'revision'):
		id_val = p.find(namespace+'id').text
		text_val = p.find(namespace+'text').text
		#print id_val,text_val
		#sha1 = sha.new(text_val)
		if text_val is not None:
			hashval = hashlib.sha1(text_val).hexdigest()
			write_str = '"'+id_val+'"'+','+'"'+hashval+'"'+','+'\n'
			f.write(write_str)
	f.close()

